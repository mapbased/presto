/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.execution.NodeScheduler.NodeSelector;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.SetThreadName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class SqlStageExecution
        implements StageExecutionNode
{
    private static final Logger log = Logger.get(SqlStageExecution.class);

    // NOTE: DO NOT call methods on the parent while holding a lock on the child.  Locks
    // are always acquired top down in the tree, so calling a method on the parent while
    // holding a lock on the 'this' could cause a deadlock.
    // This is only here to aid in debugging
    @Nullable
    private final StageExecutionNode parent;
    private final StageId stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final List<TupleInfo> tupleInfos;
    private final Map<PlanFragmentId, StageExecutionNode> subStages;
    private final Map<PlanNodeId, OutputReceiver> outputReceivers;

    private final ConcurrentMap<Node, RemoteTask> tasks = new ConcurrentHashMap<>();

    private final Optional<DataSource> dataSource;
    private final RemoteTaskFactory remoteTaskFactory;
    private final Session session; // only used for remote task factory
    private final int maxPendingSplitsPerNode;

    private final StateMachine<StageState> stageState;

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    private final Set<PlanNodeId> completeSources = new HashSet<>();

    @GuardedBy("this")
    private OutputBuffers currentOutputBuffers = new OutputBuffers(0, false);
    @GuardedBy("this")
    private OutputBuffers nextOutputBuffers;

    private OutputBuffers subStageOutputBuffers = new OutputBuffers(0, false);

    private final ExecutorService executor;

    private final Distribution getSplitDistribution = new Distribution();
    private final Distribution scheduleTaskDistribution = new Distribution();
    private final Distribution addSplitDistribution = new Distribution();

    private final NodeSelector nodeSelector;

    private Multimap<PlanNodeId,URI> exchangeLocations = ImmutableMultimap.of();

    public SqlStageExecution(QueryId queryId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int maxPendingSplitsPerNode,
            ExecutorService executor)
    {
        this(null, queryId, new AtomicInteger(), locationFactory, plan, nodeScheduler, remoteTaskFactory, session, maxPendingSplitsPerNode, executor);
    }

    private SqlStageExecution(@Nullable StageExecutionNode parent,
            QueryId queryId,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int maxPendingSplitsPerNode,
            ExecutorService executor)
    {
        checkNotNull(queryId, "queryId is null");
        checkNotNull(nextStageId, "nextStageId is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(plan, "plan is null");
        checkNotNull(nodeScheduler, "nodeScheduler is null");
        checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        checkNotNull(session, "session is null");
        Preconditions.checkArgument(maxPendingSplitsPerNode > 0, "maxPendingSplitsPerNode must be greater than 0");
        checkNotNull(executor, "executor is null");

        this.stageId = new StageId(queryId, String.valueOf(nextStageId.getAndIncrement()));
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            this.parent = parent;
            this.location = locationFactory.createStageLocation(stageId);
            this.fragment = plan.getFragment();
            this.outputReceivers = plan.getOutputReceivers();
            this.dataSource = plan.getDataSource();
            this.remoteTaskFactory = remoteTaskFactory;
            this.session = session;
            this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;
            this.executor = executor;

            tupleInfos = fragment.getTupleInfos();

            ImmutableMap.Builder<PlanFragmentId, StageExecutionNode> subStages = ImmutableMap.builder();
            for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
                PlanFragmentId subStageFragmentId = subStagePlan.getFragment().getId();
                StageExecutionNode subStage = new SqlStageExecution(this,
                        queryId,
                        nextStageId,
                        locationFactory,
                        subStagePlan,
                        nodeScheduler,
                        remoteTaskFactory,
                        session,
                        maxPendingSplitsPerNode, executor);

                subStage.addStateChangeListener(new StateChangeListener<StageInfo>()
                {
                    @Override
                    public void stateChanged(StageInfo stageInfo)
                    {
                        doUpdateState();
                    }
                });

                subStages.put(subStageFragmentId, subStage);
            }
            this.subStages = subStages.build();

            String dataSourceName = dataSource.isPresent() ? dataSource.get().getDataSourceName() : null;
            this.nodeSelector = nodeScheduler.createNodeSelector(dataSourceName, Ordering.natural().onResultOf(new Function<Node, Integer>()
            {
                @Override
                public Integer apply(Node input)
                {
                    RemoteTask task = tasks.get(input);
                    return task == null ? 0 : task.getQueuedSplits();
                }
            }));
            stageState = new StateMachine<>("stage " + stageId, this.executor, StageState.PLANNED);
            stageState.addStateChangeListener(new StateChangeListener<StageState>()
            {
                @Override
                public void stateChanged(StageState newValue)
                {
                    log.debug("Stage %s is %s", stageId, newValue);
                }
            });
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            if (stageId.equals(this.stageId)) {
                cancel(true);
            }
            else {
                for (StageExecutionNode subStage : subStages.values()) {
                    subStage.cancelStage(stageId);
                }
            }
        }
    }

    @Override
    @VisibleForTesting
    public StageState getState()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            return stageState.get();
        }
    }

    public StageInfo getStageInfo()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            List<TaskInfo> taskInfos = IterableTransformer.on(tasks.values()).transform(taskInfoGetter()).list();
            List<StageInfo> subStageInfos = IterableTransformer.on(subStages.values()).transform(stageInfoGetter()).list();

            int totalTasks = taskInfos.size();
            int runningTasks = 0;
            int completedTasks = 0;

            int totalDrivers = 0;
            int queuedDrivers = 0;
            int runningDrivers = 0;
            int completedDrivers = 0;

            long totalMemoryReservation = 0;

            long totalScheduledTime = 0;
            long totalCpuTime = 0;
            long totalUserTime = 0;
            long totalBlockedTime = 0;

            long rawInputDataSize = 0;
            long rawInputPositions = 0;

            long processedInputDataSize = 0;
            long processedInputPositions = 0;

            long outputDataSize = 0;
            long outputPositions = 0;

            for (TaskInfo taskInfo : taskInfos) {
                if (taskInfo.getState().isDone()) {
                    completedTasks++;
                }
                else {
                    runningTasks++;
                }

                TaskStats taskStats = taskInfo.getStats();

                totalDrivers += taskStats.getTotalDrivers();
                queuedDrivers += taskStats.getQueuedDrivers();
                runningDrivers += taskStats.getRunningDrivers();
                completedDrivers += taskStats.getCompletedDrivers();

                totalMemoryReservation += taskStats.getMemoryReservation().toBytes();

                totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
                totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
                totalUserTime += taskStats.getTotalUserTime().roundTo(NANOSECONDS);
                totalBlockedTime += taskStats.getTotalBlockedTime().roundTo(NANOSECONDS);

                rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
                rawInputPositions += taskStats.getRawInputPositions();

                processedInputDataSize += taskStats.getProcessedInputDataSize().toBytes();
                processedInputPositions += taskStats.getProcessedInputPositions();

                outputDataSize += taskStats.getOutputDataSize().toBytes();
                outputPositions += taskStats.getOutputPositions();
            }

            StageStats stageStats = new StageStats(
                    getSplitDistribution.snapshot(),
                    scheduleTaskDistribution.snapshot(),
                    addSplitDistribution.snapshot(),

                    totalTasks,
                    runningTasks,
                    completedTasks,

                    totalDrivers,
                    queuedDrivers,
                    runningDrivers,
                    completedDrivers,

                    new DataSize(totalMemoryReservation, BYTE).convertToMostSuccinctDataSize(),
                    new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new DataSize(rawInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                    rawInputPositions,
                    new DataSize(processedInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                    processedInputPositions,
                    new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                    outputPositions);

            return new StageInfo(stageId,
                    stageState.get(),
                    location,
                    fragment,
                    tupleInfos,
                    stageStats,
                    taskInfos,
                    subStageInfos,
                    toFailures(failureCauses));
        }
    }

    public synchronized void setOutputBuffers(OutputBuffers outputBuffers)
    {
        checkNotNull(outputBuffers, "outputBuffers is null");
        if (outputBuffers.getVersion() < currentOutputBuffers.getVersion()) {
            return;
        }
        if (nextOutputBuffers != null && outputBuffers.getVersion() < nextOutputBuffers.getVersion()) {
            return;
        }
        this.nextOutputBuffers = outputBuffers;
        this.notifyAll();
    }

    public synchronized OutputBuffers getCurrentOutputBuffers()
    {
        return currentOutputBuffers;
    }

    public synchronized OutputBuffers updateToNextOutputBuffers()
    {
        if (nextOutputBuffers == null) {
            return currentOutputBuffers;
        }

        currentOutputBuffers = nextOutputBuffers;
        nextOutputBuffers = null;
        return currentOutputBuffers;
    }

    @Override
    public void addStateChangeListener(final StateChangeListener<StageInfo> stateChangeListener)
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            stageState.addStateChangeListener(new StateChangeListener<StageState>()
            {
                @Override
                public void stateChanged(StageState newValue)
                {
                    stateChangeListener.stateChanged(getStageInfo());
                }
            });
        }
    }

    private Multimap<PlanNodeId, URI> getNewExchangeLocations()
    {
        ImmutableMultimap.Builder<PlanNodeId, URI> newExchangeLocations = ImmutableMultimap.builder();
        for (PlanNode planNode : fragment.getSources()) {
            if (planNode instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) planNode;
                for (PlanFragmentId planFragmentId : exchangeNode.getSourceFragmentIds()) {
                    StageExecutionNode subStage = subStages.get(planFragmentId);
                    Preconditions.checkState(subStage != null, "Unknown sub stage %s, known stages %s", planFragmentId, subStages.keySet());

                    // add new task locations
                    for (URI taskLocation : subStage.getTaskLocations()) {
                        if (!exchangeLocations.containsEntry(exchangeNode.getId(), taskLocation)) {
                            newExchangeLocations.putAll(exchangeNode.getId(), taskLocation);
                        }
                    }
                }
            }
        }
        return newExchangeLocations.build();
    }

    @Override
    @VisibleForTesting
    public synchronized List<URI> getTaskLocations()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            ImmutableList.Builder<URI> locations = ImmutableList.builder();
            for (RemoteTask task : tasks.values()) {
                locations.add(task.getTaskInfo().getSelf());
            }
            return locations.build();
        }
    }

    public Future<?> start()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            return scheduleStartTasks();
        }
    }

    @Override
    @VisibleForTesting
    public Future<?> scheduleStartTasks()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            // start sub-stages (starts bottom-up)
            for (StageExecutionNode subStage : subStages.values()) {
                subStage.scheduleStartTasks();
            }
            return executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    startTasks();
                }
            });
        }
    }

    private void startTasks()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            try {
                Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

                // transition to scheduling
                synchronized (this) {
                    if (!stageState.compareAndSet(StageState.PLANNED, StageState.SCHEDULING)) {
                        // stage has already been started, has been canceled or has no tasks due to partition pruning
                        return;
                    }
                }

                // schedule tasks
                if (fragment.getPartitioning() == PlanFragment.Partitioning.NONE) {
                    scheduleNotPartitioned();
                }
                else if (fragment.getPartitioning() == PlanFragment.Partitioning.HASH) {
                    scheduleHashPartitioned();
                }
                else if (fragment.getPartitioning() == PlanFragment.Partitioning.SOURCE) {
                    scheduleSourcePartitioned();
                } else {
                    throw new IllegalStateException("Unsupported partitioning: " + fragment.getPartitioning());
                }

                stageState.set(StageState.SCHEDULED);

                // tell sub stages there will be no more output buffers
                setSubStageNoMoreBufferIds();

                // add the missing exchanges output buffers
                updateNewExchangesAndBuffers(true);
            }
            catch (Throwable e) {
                // some exceptions can occur when the query finishes early
                if (!getState().isDone()) {
                    synchronized (this) {
                        failureCauses.add(e);
                        stageState.set(StageState.FAILED);
                    }
                    log.error(e, "Error while starting stage %s", stageId);
                    cancel(true);
                    throw e;
                }
                Throwables.propagateIfInstanceOf(e, Error.class);
                log.debug(e, "Error while starting stage in done query %s", stageId);
            }
            finally {
                doUpdateState();
            }
        }
    }

    private void scheduleNotPartitioned()
    {
        // create a single partition on a random node for this fragment
        scheduleTask(0, nodeSelector.selectRandomNode());
    }

    private void scheduleHashPartitioned()
    {
        // create a single partition on a random node for this fragment
        scheduleTask(0, nodeSelector.selectRandomNode());
    }

    private void scheduleSourcePartitioned()
    {
        AtomicInteger nextTaskId = new AtomicInteger(0);
        long getSplitStart = System.nanoTime();
        for (Split split : dataSource.get().getSplits()) {
            getSplitDistribution.add(System.nanoTime() - getSplitStart);

            long scheduleSplitStart = System.nanoTime();
            Node chosen = chooseNode(nodeSelector, split, nextTaskId);

            // if query has been canceled, exit cleanly; query will never run regardless
            if (getState().isDone()) {
                break;
            }

            RemoteTask task = tasks.get(chosen);
            if (task == null) {
                scheduleTask(nextTaskId.getAndIncrement(), chosen, fragment.getPartitionedSource(), split);
                scheduleTaskDistribution.add(System.nanoTime() - scheduleSplitStart);
            }
            else {
                task.addSplit(fragment.getPartitionedSource(), split);
                addSplitDistribution.add(System.nanoTime() - scheduleSplitStart);
            }

            getSplitStart = System.nanoTime();
        }
        for (RemoteTask task : tasks.values()) {
            task.noMoreSplits(fragment.getPartitionedSource());
        }
        completeSources.add(fragment.getPartitionedSource());
    }

    private Node chooseNode(NodeSelector nodeSelector, Split split, AtomicInteger nextTaskId)
    {
        while (true) {
            // if query has been canceled, exit
            if (getState().isDone()) {
                return null;
            }

            // for each split, pick the node with the smallest number of assignments
            Node chosen = nodeSelector.selectNode(split);

            // if the chosen node doesn't have too many tasks already, return
            RemoteTask task = tasks.get(chosen);
            if (task == null || task.getQueuedSplits() < maxPendingSplitsPerNode) {
                return chosen;
            }

            // if we have sub stages...
            if (!subStages.isEmpty()) {
                // before we block, we need to create all possible output buffers on the sub stages, or they can deadlock
                // waiting for the "noMoreBuffers" call
                nodeSelector.lockDownNodes();
                for (Node node : Sets.difference(new HashSet<>(nodeSelector.allNodes()), tasks.keySet())) {
                    scheduleTask(nextTaskId.getAndIncrement(), node);
                }

                // tell sub stages there will be no more output buffers
                setSubStageNoMoreBufferIds();
            }

            synchronized (this) {
                // otherwise wait for some tasks to complete
                try {
                    // todo this adds latency: replace this wait with an event listener
                    TimeUnit.SECONDS.timedWait(this, 1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }

            updateNewExchangesAndBuffers(false);
        }
    }

    private RemoteTask scheduleTask(int id, Node node)
    {
        return scheduleTask(id, node, null, null);
    }

    private RemoteTask scheduleTask(int id, Node node, PlanNodeId sourceId, Split sourceSplit)
    {
        // before scheduling a new task update all existing tasks with new exchanges and output buffers
        addNewExchangesAndBuffers();

        String nodeIdentifier = node.getNodeIdentifier();
        TaskId taskId = new TaskId(stageId, String.valueOf(id));

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        if (sourceId != null) {
            initialSplits.put(sourceId, sourceSplit);
        }
        for (Entry<PlanNodeId, URI> entry : exchangeLocations.entries()) {
            initialSplits.put(entry.getKey(), createRemoteSplitFor(node.getNodeIdentifier(), entry.getValue()));
        }

        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                taskId,
                node,
                fragment,
                initialSplits.build(),
                outputReceivers,
                currentOutputBuffers);

        task.addStateChangeListener(new StateChangeListener<TaskInfo>()
        {
            @Override
            public void stateChanged(TaskInfo taskInfo)
            {
                doUpdateState();
            }
        });

        // create and update task
        task.start();

        // record this task
        tasks.put(node, task);

        // update in case task finished before listener was registered
        doUpdateState();

        // stop if stage is already done
        if (getState().isDone()) {
            return task;
        }

        // tell the sub stages to create a buffer for this task
        addSubStageBufferId(nodeIdentifier);

        return task;
    }

    private void updateNewExchangesAndBuffers(boolean waitUntilFinished)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not add exchanges or buffers to tasks while holding a lock on this");

        while (!getState().isDone()) {
            boolean finished = addNewExchangesAndBuffers();

            if (finished || !waitUntilFinished) {
                return;
            }

            waitForNewExchangesOrBuffers();
        }
    }

    private boolean addNewExchangesAndBuffers()
    {
        // get new exchanges and update exchange state
        Set<PlanNodeId> completeSources = updateCompleteSources();
        boolean allSourceComplete = completeSources.containsAll(fragment.getSourceIds());
        Multimap <PlanNodeId, URI> newExchangeLocations = getNewExchangeLocations();
        exchangeLocations = ImmutableMultimap.<PlanNodeId, URI>builder()
                .putAll(exchangeLocations)
                .putAll(newExchangeLocations)
                .build();

        // get new output buffer and update output buffer state
        OutputBuffers outputBuffers = updateToNextOutputBuffers();

        // finished state must be decided before update to avoid race conditions
        boolean finished = allSourceComplete && outputBuffers.isNoMoreBufferIds();

        // update tasks
        for (RemoteTask task : tasks.values()) {
            for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
                RemoteSplit remoteSplit = createRemoteSplitFor(task.getNodeId(), entry.getValue());
                task.addSplit(entry.getKey(), remoteSplit);
            }
            task.setOutputBuffers(outputBuffers);
            for (PlanNodeId completeSource : completeSources) {
                task.noMoreSplits(completeSource);
            }
        }

        return finished;
    }

    private synchronized void waitForNewExchangesOrBuffers()
    {
        while (!getState().isDone()) {
            // if next loop will finish, don't wait
            Set<PlanNodeId> completeSources = updateCompleteSources();
            boolean allSourceComplete = completeSources.containsAll(fragment.getSourceIds());
            if (allSourceComplete && getCurrentOutputBuffers().isNoMoreBufferIds()) {
                return;
            }
            // do we have a new set of output buffers?
            synchronized (this) {
                if (nextOutputBuffers != null) {
                    return;
                }
            }
            // do we have new exchange locations?
            if (!getNewExchangeLocations().isEmpty()) {
                return;
            }
            // wait for a state change
            try {
                TimeUnit.SECONDS.timedWait(this, 1);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

    private Set<PlanNodeId> updateCompleteSources()
    {
        for (PlanNode planNode : fragment.getSources()) {
            if (!completeSources.contains(planNode.getId()) && planNode instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) planNode;
                boolean exchangeFinished = true;
                for (PlanFragmentId planFragmentId : exchangeNode.getSourceFragmentIds()) {
                    StageExecutionNode subStage = subStages.get(planFragmentId);
                    switch (subStage.getState()) {
                        case PLANNED:
                        case SCHEDULING:
                            exchangeFinished = false;
                            break;
                    }
                }
                if (exchangeFinished) {
                    completeSources.add(planNode.getId());
                }
            }
        }
        return completeSources;
    }

    @VisibleForTesting
    public void doUpdateState()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not doUpdateState while holding a lock on this");

        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            synchronized (this) {
                // wake up worker thread waiting for state changes
                this.notifyAll();

                StageState currentState = stageState.get();
                if (currentState.isDone()) {
                    return;
                }

                List<StageState> subStageStates = ImmutableList.copyOf(transform(transform(subStages.values(), stageInfoGetter()), stageStateGetter()));
                if (any(subStageStates, equalTo(StageState.FAILED))) {
                    stageState.set(StageState.FAILED);
                }
                else {
                    List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks.values(), taskInfoGetter()), taskStateGetter()));
                    if (any(taskStates, equalTo(TaskState.FAILED))) {
                        stageState.set(StageState.FAILED);
                    }
                    else if (currentState != StageState.PLANNED && currentState != StageState.SCHEDULING) {
                        // all tasks are now scheduled, so we can check the finished state
                        if (all(taskStates, TaskState.inDoneState())) {
                            stageState.set(StageState.FINISHED);
                        }
                        else if (any(taskStates, equalTo(TaskState.RUNNING))) {
                            stageState.set(StageState.RUNNING);
                        }
                    }
                }
            }

            if (stageState.get().isDone()) {
                // finish tasks and stages
                cancel(false);
            }
        }
    }

    public void cancel(boolean force)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)) {
            // before canceling the task wait to see if it finishes normally
            if (!force) {
                Duration waitTime = new Duration(100, MILLISECONDS);
                for (RemoteTask remoteTask : tasks.values()) {
                    try {
                        waitTime = remoteTask.waitForTaskToFinish(waitTime);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
            }
            // check if the task completed naturally
            doUpdateState();

            // transition to canceled state, only if not already finished
            synchronized (this) {
                if (!stageState.get().isDone()) {
                    log.debug("Cancelling stage %s", stageId);
                    stageState.set(StageState.CANCELED);
                }
            }

            // make sure all tasks are done
            for (RemoteTask task : tasks.values()) {
                task.cancel();
            }

            // propagate update to tasks and stages
            for (StageExecutionNode subStage : subStages.values()) {
                subStage.cancel(force);
            }
        }
    }

    private void addSubStageBufferId(String bufferId)
    {
        subStageOutputBuffers = new OutputBuffers(
                subStageOutputBuffers.getVersion() + 1,
                subStageOutputBuffers.isNoMoreBufferIds(),
                ImmutableSet.<String>builder().addAll(subStageOutputBuffers.getBufferIds()).add(bufferId).build());
        for (StageExecutionNode subStage : subStages.values()) {
            subStage.setOutputBuffers(subStageOutputBuffers);
        }
    }

    private void setSubStageNoMoreBufferIds()
    {
        subStageOutputBuffers = new OutputBuffers(
                subStageOutputBuffers.getVersion() + 1,
                true,
                subStageOutputBuffers.getBufferIds());
        for (StageExecutionNode subStage : subStages.values()) {
            subStage.setOutputBuffers(subStageOutputBuffers);
        }
    }

    private RemoteSplit createRemoteSplitFor(String nodeId, URI taskLocation)
    {
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(nodeId).build();
        return new RemoteSplit(splitLocation, tupleInfos);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("stageId", stageId)
                .add("location", location)
                .add("stageState", stageState.get())
                .toString();
    }

    public static Function<RemoteTask, TaskInfo> taskInfoGetter()
    {
        return new Function<RemoteTask, TaskInfo>()
        {
            @Override
            public TaskInfo apply(RemoteTask remoteTask)
            {
                return remoteTask.getTaskInfo();
            }
        };
    }

    public static Function<StageExecutionNode, StageInfo> stageInfoGetter()
    {
        return new Function<StageExecutionNode, StageInfo>()
        {
            @Override
            public StageInfo apply(StageExecutionNode stage)
            {
                return stage.getStageInfo();
            }
        };
    }
}

/*
 * Since the execution is a tree of SqlStateExecutions, each stage can directly access
 * the private fields and methods of stages up and down the tree.  To prevent accidental
 * errors, each stage reference parents and children using this interface so direct
 * access is not possible.
 */
interface StageExecutionNode
{
    StageInfo getStageInfo();

    StageState getState();

    Future<?> scheduleStartTasks();

    void setOutputBuffers(OutputBuffers outputBuffers);

    Iterable<? extends URI> getTaskLocations();

    void addStateChangeListener(StateChangeListener<StageInfo> stateChangeListener);

    void cancelStage(StageId stageId);

    void cancel(boolean force);
}


