import React, { useMemo, useState, useEffect, useRef } from 'react';

//import json_data from './big_multi.json';// assert { type: "json" };
import { ExportTraceServiceRequest, Event, Resource, parseFromJson, KeyValue, AttributeValue } from './otel';

const PARAMS = {
    topBarHeight: 30,
    timelineHeight: 70,
    endBarWidth: 10,
    searchBarHeight: 30,
    displayedTimesHeight: 30,
    nodeNameWidth: 120,
    spanHeight: 30,
    timePointWidth: 230,
};

// Representation of a Span used by the visualizer
type VisSpan = {
    spanId: string,
    name: string,
    startTime: number,
    endTime: number,
    parentSpanId: string,
    children: VisSpan[],
    events: Event[],
    resource: Resource,
    attributes: KeyValue[],
    height_level: number,
}

type VisNode = {
    name: string,
    attributes: { [key: string]: string },
}

type NodeSpans = {
    node: VisNode,
    spans: VisSpan[],
    bbox: SpanBoundingBox,
}

// Convert raw otel data to VisSpans
function extractVisSpans(otel_requests: ExportTraceServiceRequest[]): VisSpan[] {
    let spans: VisSpan[] = [];
    for (let request of otel_requests) {
        for (let resource_spans of request.resourceSpans) {
            for (let scope_spans of resource_spans.scopeSpans) {
                for (let span of scope_spans.spans) {
                    spans.push({
                        spanId: span.spanId,
                        name: span.name,
                        startTime: span.startTimeUnixNano,
                        endTime: span.endTimeUnixNano,
                        parentSpanId: span.parentSpanId,
                        children: [],
                        events: span.events,
                        resource: resource_spans.resource,
                        attributes: span.attributes,
                        height_level: 0,
                    });
                }
            }
        }
    }
    console.log("Number of spans: ", spans.length);

    // Map from spanId to span
    let spanIdToSpan: { [spanId: string]: VisSpan } = {};
    for (let span of spans) {
        spanIdToSpan[span.spanId] = span;
    }

    for (let span of spans) {
        if (span.parentSpanId in spanIdToSpan) {
            spanIdToSpan[span.parentSpanId].children.push(span);
        }
    }

    // Sort children by start time
    for (let span of spans) {
        span.children.sort((a, b) => a.startTime - b.startTime);
    }

    // Spans whose parent_id is null
    let top_level_spans: VisSpan[] = [];
    for (let span of spans) {
        if (span.parentSpanId === "") {
            top_level_spans.push(span);
        }
    }
    console.log("Number of top level spans:", top_level_spans.length);

    return top_level_spans;
}

class AllSpans {
    spans: VisSpan[];

    constructor(spans: VisSpan[]) {
        this.spans = spans;
    }

    getSpansForTimeRange(startTime: number, endTime: number): VisSpan[] {
        // TODO - take children into account
        // TODO - optimize
        return this.spans.filter(span => isCollidingInAxis(span.startTime, span.endTime, startTime, endTime));
    }
}

enum SpanFilterDecision {
    Keep,
    DeleteSpanAndChildren,
}

function filterSpans(spans: VisSpan[], filter: (span: VisSpan) => SpanFilterDecision): VisSpan[] {
    let res: VisSpan[] = [];
    for (let span of spans) {
        let decision = filter(span);
        if (decision === SpanFilterDecision.Keep) {
            // filter children

            res.push(span);
        } else if (decision === SpanFilterDecision.DeleteSpanAndChildren) {
            // Do nothing
        } else {
            throw new Error("Unknown SpanFilterDecision");
        }
    }
    return res;
}

// So basically the idea is to
// 1. Extract all unique resources
// 2. For each resource, extract all spans that belong to that resource
// 3. Place this resource's spans on the timeline, this will tell us where to start the next resource's timeline
// 4. Repeat for the next resource

function getNodeSpans(top_level_spans: VisSpan[]): NodeSpans[] {
    let nodes_by_name: { [name: string]: VisNode } = {};
    let spans_by_node_name: { [name: string]: VisSpan[] } = {};

    for (let span of top_level_spans) {
        //console.log(span.resource)
        let attrs: KeyValue[] = span.resource.attributes;
        let maybe_service_name: KeyValue | undefined = attrs.find(kv => kv.key === "service.name");
        let service_name = maybe_service_name ? maybe_service_name.value.stringValue : "service.name missing";

        if (!(service_name in nodes_by_name)) {
            nodes_by_name[service_name] = {
                name: service_name,
                attributes: {}, // TODO
            };
        }

        // Add span to spans_by_node_name, create an empty list if it doesn't exist
        if (service_name in spans_by_node_name) {
            spans_by_node_name[service_name].push(span);
        } else {
            spans_by_node_name[service_name] = [span];
        }
    }

    // Sort by name
    let all_nodes = Object.values(nodes_by_name);
    all_nodes.sort((a, b) => a.name.localeCompare(b.name));

    let res: NodeSpans[] = [];
    for (let node of all_nodes) {
        let cur_spans = spans_by_node_name[node.name];
        cur_spans.sort((a, b) => a.startTime - b.startTime);
        let bbox = arrangeSpans(cur_spans);
        res.push({
            node: node,
            spans: cur_spans,
            bbox: bbox,
        });
    }

    return res;
}

type SpanBoundingBox = {
    start_time: number,
    end_time: number,
    height: number,
}

// Takes a bunch of spans and modifies their heights to avoid collisions
function arrangeSpans(spans: VisSpan[]): SpanBoundingBox {
    if (spans.length === 0) {
        return {
            start_time: 0,
            end_time: 0,
            height: 0,
        };
    }

    // 1. Run arrangeSpans for every span, get bounding box for every span
    // 2. Modify root span heights to avoid collisions of bounding boxes

    let span_bounding_boxes: SpanBoundingBox[] = [];
    for (let span of spans) {
        let span_bbox = arrangeSpan(span);
        span_bounding_boxes.push(span_bbox);
    }

    for (let i = 0; i < spans.length; i++) {
        let cur_span = spans[i];
        let cur_span_bbox = span_bounding_boxes[i];

        // TODO - this is n^2 (^3?) - optimize
        while (true) {
            let is_colliding = false;

            for (let j = 0; j < i; j++) {
                if (isColliding(cur_span, cur_span_bbox, spans[j], span_bounding_boxes[j])) {
                    is_colliding = true;
                    break;
                }
            }

            if (is_colliding) {
                cur_span.height_level += 1;
            } else {
                break;
            }
        }
    }

    let final_bbox = span_bounding_boxes[0];
    for (let i = 0; i < spans.length; i++) {
        let cur_span = spans[i];
        let bbox = span_bounding_boxes[i];

        final_bbox.start_time = Math.min(final_bbox.start_time, bbox.start_time);
        final_bbox.end_time = Math.max(final_bbox.end_time, bbox.end_time);
        final_bbox.height = Math.max(final_bbox.height, cur_span.height_level + bbox.height);
    }

    return final_bbox;
}

function arrangeSpan(span: VisSpan): SpanBoundingBox {
    if (span.children.length === 0) {
        return {
            start_time: span.startTime,
            end_time: span.endTime,
            height: 1,
        };
    }

    let bbox = arrangeSpans(span.children);
    bbox.start_time = Math.min(span.startTime, bbox.start_time);
    bbox.end_time = Math.max(span.endTime, bbox.end_time);
    bbox.height += 1;

    return bbox;
}


function isBetween(coord: number, start: number, end: number): boolean {
    return coord >= start && coord <= end;
}

function isCollidingInAxis(begin1: number, end1: number, begin2: number, end2: number): boolean {
    return isBetween(begin2, begin1, end1) || isBetween(end2, begin1, end1) || isBetween(begin1, begin2, end2) || isBetween(end1, begin2, end2);
}

function isColliding(span1: VisSpan, bbox1: SpanBoundingBox, span2: VisSpan, bbox2: SpanBoundingBox): boolean {
    // Are the times colliding?
    let is_time_colliding = isCollidingInAxis(span1.startTime, span1.endTime, span2.startTime, span2.endTime);

    if (!is_time_colliding) {
        return false;
    }

    // Are the heights colliding?
    return isCollidingInAxis(span1.height_level, span1.height_level + bbox1.height, span2.height_level, span2.height_level + bbox2.height);
}

class TaskTimer {
    start_time: number;
    task: string;

    constructor(task: string) {
        console.log("Task:", task, "- starting");
        this.task = task;
        this.start_time = Date.now();
    }

    stop() {
        let end_time = Date.now();
        console.log("Task:", this.task, "- done in ", end_time - this.start_time, "ms");
    }
}

class TimelineState {
    startTime: number;
    endTime: number;
    selectedStart: number;
    selectedEnd: number;
    minStartTime: number;
    maxEndTime: number;

    action: TimelineAction;
    deltaX: number;
    notDraggedEdgeTime: number;

    constructor() {
        this.startTime = 1e9;
        this.endTime = 3e9;
        this.selectedStart = 1.5e9;
        this.selectedEnd = 2.5e9;
        this.minStartTime = 0;
        this.maxEndTime = 4e9;
        this.action = TimelineAction.None;
        this.deltaX = 0;
        this.notDraggedEdgeTime = 0;
    }
}

enum TimelineAction {
    None,
    DraggingEdge,
    DraggingMiddle,
}

function screenToTime(screenX: number, timeline: TimelineState, windowWidth: number): number {
    return timeline.startTime + screenX / windowWidth * (timeline.endTime - timeline.startTime);
}

function timeToScreen(time: number, timeline: TimelineState, windowWidth: number): number {
    return (time - timeline.startTime) / (timeline.endTime - timeline.startTime) * windowWidth;
}

function makeTimelineDragHandlers(timelineState: TimelineState, setTimelineState: (timelineState: TimelineState) => void, windowWidth: number): { onMouseUp: (e: React.MouseEvent) => void, onMouseMove: (e: React.MouseEvent) => void } {
    let onMouseMove = (e: React.MouseEvent) => {
        let mouseX = e.clientX

        if (timelineState.action === TimelineAction.DraggingEdge) {
            let draggedEdgeTime = screenToTime(mouseX - timelineState.deltaX, timelineState, windowWidth);
            let newSelectedStart = Math.min(draggedEdgeTime, timelineState.notDraggedEdgeTime);
            let newSelectedEnd = Math.max(draggedEdgeTime, timelineState.notDraggedEdgeTime);
            setTimelineState({ ...timelineState, selectedStart: newSelectedStart, selectedEnd: newSelectedEnd });
        } else if (timelineState.action === TimelineAction.DraggingMiddle) {
            let newSelectedStart = screenToTime(mouseX - timelineState.deltaX, timelineState, windowWidth);
            let newSelectedEnd = newSelectedStart + (timelineState.selectedEnd - timelineState.selectedStart);
            setTimelineState({ ...timelineState, selectedStart: newSelectedStart, selectedEnd: newSelectedEnd });
        }
    };

    let onMouseUp = (e: React.MouseEvent) => {
        setTimelineState({ ...timelineState, action: TimelineAction.None });
    };

    return { onMouseMove: onMouseMove, onMouseUp: onMouseUp };
}

type DisplayedSpansRange = {
    startTime: number,
    endTime: number,
}

export const TraceVisualizer = () => {
    const { windowWidth, windowHeight } = useWindowDimensions();

    let [traceFile, setTraceFile] = useState<string>("");
    let [timelineState, setTimelineState] = useState<TimelineState>(new TimelineState());
    let [displayedSpansRange, setDisplayedSpansRange] = useState<DisplayedSpansRange>({ startTime: timelineState.selectedStart, endTime: timelineState.selectedEnd });


    console.log("timelineState is ", timelineState);

    let lastUpdatedDisplayedSpansRange = useRef(Date.now());
    let updateDisplayedSpansRangePeriod = 100;
    useEffect(() => {
        if (displayedSpansRange.startTime == timelineState.selectedStart && displayedSpansRange.endTime == timelineState.selectedEnd) {
            return;
        }

        let now = Date.now();
        let lastUpdate = lastUpdatedDisplayedSpansRange.current;
        if (now - lastUpdate >= updateDisplayedSpansRangePeriod) {
            console.log("Updating displayed spans range");
            setDisplayedSpansRange({ startTime: timelineState.selectedStart, endTime: timelineState.selectedEnd });
            lastUpdatedDisplayedSpansRange.current = now;
            return () => { };
        }

        const scheduledUpdate = setTimeout(() => {
            console.log("Updating displayed spans range");
            setDisplayedSpansRange({ startTime: timelineState.selectedStart, endTime: timelineState.selectedEnd });
            lastUpdatedDisplayedSpansRange.current = Date.now();
        }, lastUpdate + updateDisplayedSpansRangePeriod - now);

        return () => clearTimeout(scheduledUpdate);
    });

    let { onMouseMove, onMouseUp } = makeTimelineDragHandlers(timelineState, setTimelineState, windowWidth);

    let topBarHeight = 30;
    let timelineHeight = 70;
    let searchBarHeight = 30;
    let tracesHeight = windowHeight - topBarHeight - timelineHeight - searchBarHeight;

    const { allSpans, minStartTime, maxEndTime
    } = useMemo(() => {
        if (traceFile === "") {
            return {
                allSpans: new AllSpans([]),
                minStartTime: timelineState.minStartTime,
                maxEndTime: timelineState.maxEndTime,
            };
        }

        console.log("Parsing file...");

        let t = new TaskTimer("Parsing txt -> json");
        let file_json = JSON.parse(traceFile);
        t.stop();

        t = new TaskTimer("Parsing json -> data");
        let parsed_data = parseFromJson(file_json);
        t.stop();

        t = new TaskTimer("Extracting VisSpans");
        let spans: VisSpan[] = extractVisSpans(parsed_data);
        t.stop();

        let minStartTime = Math.min(...spans.map(span => span.startTime));
        let maxEndTime = Math.max(...spans.map(span => span.endTime));

        t = new TaskTimer("Creating AllSpans");
        let allSpans = new AllSpans(spans);
        t.stop();

        return {
            allSpans: allSpans,
            minStartTime: minStartTime,
            maxEndTime: maxEndTime,
        };
    }, [traceFile]);

    if (minStartTime !== timelineState.minStartTime || maxEndTime !== timelineState.maxEndTime) {
        console.log("Initializing timelineState after loading trace file");
        setTimelineState({
            ...timelineState,
            minStartTime: minStartTime,
            maxEndTime: maxEndTime,
            startTime: minStartTime,
            endTime: maxEndTime,
            selectedStart: minStartTime,
            selectedEnd: Math.min(minStartTime + 0.100 * 1e9, maxEndTime),
        });
    }

    let spansToDisplay: VisSpan[] = useMemo(() => {
        let t = new TaskTimer("Getting spans within camera time range");
        let res = allSpans.getSpansForTimeRange(displayedSpansRange.startTime, displayedSpansRange.endTime);
        console.log("Number of top-level spans within camera time range:", res.length);
        t.stop();
        return res;
    }, [allSpans, displayedSpansRange]);


    /*
    let filter = (span: VisSpan) => SpanFilterDecision.Keep;

    let filteredSpans: VisSpan[] = useMemo(() => {
        let t = new TaskTimer("Filtering spans");
        console.log("Number of top-level spans before filtering:", spansToDisplay.length);
        let res = filterSpans(spansToDisplay, filter);
        console.log("Number of top-level spans after filtering:", res.length);
        t.stop();
        return res;
    }, [spansToDisplay, filter]);
    */

    let nodeSpans = useMemo(() => {
        let t = new TaskTimer("Getting node spans");
        let res = getNodeSpans(spansToDisplay);
        t.stop();
        return res;
    }, [spansToDisplay]);

    let renderTimer: TaskTimer | null = null;
    let startRenderTimer = () => {
        renderTimer = new TaskTimer("Rendering");
    }
    let stopRenderTimer = () => {
        if (renderTimer) {
            renderTimer.stop();
        }
    }

    console.log("Number of displayed spans: ", countDisplayedSpans(nodeSpans));
    return (<div onMouseMove={onMouseMove} onMouseUp={onMouseUp}>
        <StartRenderTimer startRenderTimer={startRenderTimer} />
        <TopBar topBarHeight={topBarHeight} setTraceFile={setTraceFile} />
        <Timeline width={windowWidth} timelineState={timelineState} setTimelineState={setTimelineState} />
        <SearchBar />
        <DrawAllNodeSpans nodeSpans={nodeSpans} tracesHeight={tracesHeight} minStartTime={minStartTime} displayedSpansRange={displayedSpansRange} windowWidth={windowWidth} />
        <StopRenderTimer stopRenderTimer={stopRenderTimer} />
    </div >);
}

function StartRenderTimer({ startRenderTimer }: { startRenderTimer: () => void }): JSX.Element {
    startRenderTimer();
    return <></>;
}

function StopRenderTimer({ stopRenderTimer }: { stopRenderTimer: () => void }): JSX.Element {
    stopRenderTimer();
    return <></>;
}

function TopBar({ topBarHeight, setTraceFile }: { topBarHeight: number, setTraceFile: (traceFile: string) => void }): JSX.Element {
    return <div style={{ height: topBarHeight, border: "1px solid black", background: "lightgrey" }}>
        <input type="file" onChange={(e) => {
            const input: HTMLInputElement | null = e.currentTarget;
            if (input && input.files) {
                const file = input.files[0];
                const reader = new FileReader();
                reader.onload = (e) => {
                    setTraceFile(e.target!.result as string);
                };
                reader.readAsText(file);
            }
        }} />
    </div>
}

function Timeline({ width, timelineState, setTimelineState }: { width: number, timelineState: TimelineState, setTimelineState: (timelineState: TimelineState) => void }): JSX.Element {
    let selectedStartX = timeToScreen(timelineState.selectedStart, timelineState, width);
    let selectedEndX = timeToScreen(timelineState.selectedEnd, timelineState, width);
    let selectedWidth = selectedEndX - selectedStartX;

    let leftBarOnMouseDown = (e: React.MouseEvent) => {
        if (e.button !== 0) {
            return;
        }
        if (timelineState.action !== TimelineAction.None) {
            return;
        }
        e.stopPropagation();
        e.preventDefault();
        setTimelineState({ ...timelineState, action: TimelineAction.DraggingEdge, deltaX: e.clientX - selectedStartX, notDraggedEdgeTime: timelineState.selectedEnd });
    };

    let rightBarOnMouseDown = (e: React.MouseEvent) => {
        if (e.button !== 0) {
            return;
        }
        if (timelineState.action !== TimelineAction.None) {
            return;
        }
        e.stopPropagation();
        e.preventDefault();
        setTimelineState({ ...timelineState, action: TimelineAction.DraggingEdge, deltaX: e.clientX - selectedEndX, notDraggedEdgeTime: timelineState.selectedStart });
    };

    let middleBarOnMouseDown = (e: React.MouseEvent) => {
        if (e.button !== 0) {
            return;
        }
        if (timelineState.action !== TimelineAction.None) {
            return;
        }
        e.stopPropagation();
        e.preventDefault();
        setTimelineState({ ...timelineState, action: TimelineAction.DraggingMiddle, deltaX: e.clientX - selectedStartX });
    }

    let backgroundOnMouseDown = (e: React.MouseEvent) => {
        if (e.button !== 0) {
            return;
        }
        if (timelineState.action !== TimelineAction.None) {
            return;
        }
        e.stopPropagation();
        e.preventDefault();
        let clickedTime = screenToTime(e.clientX, timelineState, width);
        setTimelineState({ ...timelineState, action: TimelineAction.DraggingEdge, selectedStart: clickedTime, selectedEnd: clickedTime, deltaX: 0, notDraggedEdgeTime: clickedTime });
    };

    let timelineScrollHandler = (e: React.WheelEvent) => {
        // e.preventDefault(); Hard to make it work :/
        // See https://stackoverflow.com/questions/63663025/react-onwheel-handler-cant-preventdefault-because-its-a-passive-event-listener
        e.stopPropagation();

        let mouseX = e.clientX;
        let mouseTime = screenToTime(mouseX, timelineState, width);

        let speedFactor = 0.001;
        let scrollAmount = e.deltaY * speedFactor;
        console.log("Scrolling! (amount: ", scrollAmount, ")");

        let currentLength = timelineState.endTime - timelineState.startTime;
        let newLength = currentLength * (1 + scrollAmount);

        let newStart = mouseTime - (mouseTime - timelineState.startTime) * newLength / currentLength;
        let newEnd = newStart + newLength;

        newStart = Math.max(timelineState.minStartTime, newStart);
        newEnd = Math.min(timelineState.maxEndTime, newEnd);

        setTimelineState({ ...timelineState, startTime: newStart, endTime: newEnd });
    };

    let middleBarWidth = Math.max(selectedWidth - PARAMS.endBarWidth, 0);
    let middleBarColor = "blue";
    let endBarsColor = "brown";

    return <div
        style={{ height: PARAMS.timelineHeight, position: "relative", border: "1px solid black", overflow: "hidden", background: "lightblue" }}
        onMouseDown={backgroundOnMouseDown}
        onWheel={timelineScrollHandler}>

        <TimePoints startTime={timelineState.startTime} endTime={timelineState.endTime} width={width} minStartTime={timelineState.minStartTime} />

        <div style={{ position: "absolute", left: selectedStartX + PARAMS.endBarWidth / 2, top: 0, width: middleBarWidth, height: PARAMS.timelineHeight, background: middleBarColor, opacity: 0.3 }} onMouseDown={middleBarOnMouseDown}
        > </div>

        <div style={{ position: "absolute", left: selectedStartX - PARAMS.endBarWidth / 2, top: 0, width: PARAMS.endBarWidth, height: PARAMS.timelineHeight, background: endBarsColor, opacity: 0.3 }} onMouseDown={leftBarOnMouseDown}></div>

        <div style={{ position: "absolute", left: selectedStartX + selectedWidth - PARAMS.endBarWidth / 2, top: 0, width: PARAMS.endBarWidth, height: PARAMS.timelineHeight, background: endBarsColor, opacity: 0.3 }} onMouseDown={rightBarOnMouseDown}></div>
    </div >
}

function TimePoints({ startTime, endTime, width, minStartTime }: { startTime: number, endTime: number, width: number, minStartTime: number }): JSX.Element {
    console.log("Generating time points from ", startTime, " to ", endTime, " with width ", width);

    let numPoints = Math.floor(width / PARAMS.timePointWidth);
    let points = [];
    for (let i = 0; i < numPoints; i++) {
        let time = startTime + (endTime - startTime) / numPoints * i;
        let time_utc = new Date(time / 1e6).toISOString().replace("T", " ").replace("Z", "");
        let seconds_since_start = "" + ((time - minStartTime) / 1e9).toFixed(3) + "s";

        let screenPos = (time - startTime) / (endTime - startTime) * width;

        points.push(<div key={i} style={{ position: "absolute", left: screenPos, top: 0, width: PARAMS.timePointWidth, height: 20, borderLeft: "2px solid black" }}>
            <div>{time_utc}</div>
            <div>{seconds_since_start}</div>
        </div>);
    }

    console.log("Generated ", numPoints, " time points");

    return <div style={{ position: "relative", width: width }}>
        {points}
    </div>
}

function SearchBar(): JSX.Element {
    return <div style={{ height: PARAMS.searchBarHeight, border: "1px solid black", background: "lightgrey" }}>
        This is the search bar (TODO)
    </div>
}

function DrawAllNodeSpans({ nodeSpans, tracesHeight, minStartTime, displayedSpansRange, windowWidth }: { nodeSpans: NodeSpans[], tracesHeight: number, minStartTime: number, windowWidth: number, displayedSpansRange: DisplayedSpansRange }): JSX.Element {
    let timeToScreen = (time: number) => (time - displayedSpansRange.startTime) / (displayedSpansRange.endTime - displayedSpansRange.startTime) * (windowWidth - PARAMS.nodeNameWidth);

    let [clickedSpan, setClickedSpan] = useState<VisSpan | null>(null);
    let [includeChildEvents, setIncludeChildEvents] = useState(true);

    return (<div>
        <div style={{ height: 50, marginLeft: PARAMS.nodeNameWidth, borderBottom: "1px solid black" }}> <TimePoints startTime={displayedSpansRange.startTime} endTime={displayedSpansRange.endTime} width={windowWidth - PARAMS.nodeNameWidth} minStartTime={minStartTime} /> </div>
        <div style={{ height: tracesHeight - 50, overflowX: "hidden", overflowY: "scroll" }}>
            {nodeSpans.map(ns => <DrawNodeSpans nodeSpans={ns} width={windowWidth} timeToScreen={timeToScreen} setClickedSpan={setClickedSpan} />)}
        </div>
        {clickedSpan && ClickedSpanInfo({ span: clickedSpan, resetClickedSpan: () => setClickedSpan(null), maxWidth: windowWidth, maxHeight: tracesHeight, includeChildEvents: includeChildEvents, setIncludeChildEvents: setIncludeChildEvents })}
    </div>);
}

type HoveredSpan = {
    span: VisSpan,
}

function DrawNodeSpans({ nodeSpans, width, timeToScreen, setClickedSpan }: { nodeSpans: NodeSpans, width: number, timeToScreen: (time: number) => number, setClickedSpan: (span: VisSpan | null) => void }): JSX.Element {
    let height = nodeSpans.bbox.height * PARAMS.spanHeight + 10;
    return (<div style={{ position: "relative", height: height }}>
        <div style={{ width: PARAMS.nodeNameWidth, height: height, border: "1px solid black" }}> {nodeSpans.node.name} </div>
        <div style={{ position: "absolute", left: PARAMS.nodeNameWidth, top: 0, width: width - PARAMS.nodeNameWidth, height: height }}>
            <div style={{ position: "relative", display: "inline-block", border: "1px solid black", width: width - PARAMS.nodeNameWidth, height: height, overflowX: "hidden" }}>
                {nodeSpans.spans.map(s => <DrawSpan span={s} timeToScreen={timeToScreen} curHeightLevel={0} setClickedSpan={setClickedSpan} />)}
            </div>
        </div>
    </div >

    )
}

function ClickedSpanInfo({ span, resetClickedSpan, maxHeight, maxWidth, includeChildEvents, setIncludeChildEvents }: {
    span: VisSpan, resetClickedSpan: () => void, maxHeight: number, maxWidth: number, includeChildEvents: boolean, setIncludeChildEvents: (includeChildEvents: boolean) => void

}): JSX.Element {
    let events = includeChildEvents ? collectEvents(span) : span.events;
    events.sort((a, b) => a.timeUnixNano - b.timeUnixNano);

    return (
        <div style={{
            position: "absolute", top: 0, left: 0, background: "white", border: "2px solid black", margin: 10, padding: 10, marginTop: 60
            , maxHeight: maxHeight, maxWidth: maxWidth, overflowY: "scroll"
        }}>
            <button onClick={resetClickedSpan}>Close</button>
            <div> <h2> {span.name} </h2></div>
            <div> {((span.endTime - span.startTime) / 1e6).toFixed(3)} ms </div>
            <div> {new Date(span.startTime / 1e6).toISOString()} - {new Date(span.endTime / 1e6).toISOString()} </div>
            <ul>
                {span.attributes.map(kv => <li>{kv.key}: {getValueAsStr(kv.value)}</li>)}
            </ul>
            <div style={{ paddingLeft: 10 }}>
                <h2> Events </h2>
                {
                    includeChildEvents &&
                    <div key={0} style={{ background: "lightgreen" }}> Including events from child spans <button onClick={() => setIncludeChildEvents(false)}> Click to exclude </button> </div>}
                {!includeChildEvents && <div key={1} style={{ background: "rgb(255 204 203)" }}> Not including events from child spans <button onClick={() => setIncludeChildEvents(true)}> Click to include </button> </div>}
                <div>
                    {events.map(event => DisplayEvent({ event: event }))}
                    {events.length === 0 && <div>No events</div>}
                </div>
            </div>
        </div >
    )
}

function collectEvents(span: VisSpan): Event[] {
    let res: Event[] = [];
    res.push(...span.events);
    for (let child of span.children) {
        res.push(...collectEvents(child));
    }
    return res;
}

function DisplayEvent({ event }: { event: Event }): JSX.Element {
    return <div style={{ border: "1px solid grey", padding: 10 }}>
        <div>{new Date(event.timeUnixNano / 1e6).toISOString()} - "{event.name}"</div>
        <ul>{event.attributes.map(kv => {
            console.log("KV: ", kv);
            return <li>{kv.key}: {getValueAsStr(kv.value)}</li>
        })}</ul>
    </div>
}

function getValueAsStr(val: AttributeValue): string {
    // Take the attribute that exists - int or string or double
    if (val.stringValue !== undefined) {
        return val.stringValue;
    } else if (val.doubleValue !== undefined) {
        return "" + val.doubleValue;
    } else if (val.intValue !== undefined) {
        return "" + val.intValue;
    } else if (val.boolValue !== undefined) {
        return "" + val.boolValue;
    } else {
        return "Unknown type";
    }
}

function DrawSpan({ span, timeToScreen, curHeightLevel, setClickedSpan }: { span: VisSpan, timeToScreen: (time: number) => number, curHeightLevel: number, setClickedSpan: (span: VisSpan) => void }): JSX.Element {
    let x = timeToScreen(span.startTime);
    let width = timeToScreen(span.endTime) - x;
    let y = (curHeightLevel + span.height_level) * PARAMS.spanHeight;

    return (
        <div>
            <div style={{ position: "absolute", left: x, top: y, height: PARAMS.spanHeight, background: "white", color: "transparent", fontFamily: "monospace" }}
                onClick={() => setClickedSpan(span)}>
                {span.name}
            </div>
            <div style={{ position: "absolute", left: x, top: y, width: width, height: PARAMS.spanHeight, border: "1px solid orange", background: "yellow", }}
                onClick={() => setClickedSpan(span)}
            >
            </div>
            <div style={{ position: "absolute", left: x, top: y, fontFamily: "monospace" }}
                onClick={() => setClickedSpan(span)}>
                {span.name}
            </div >
            {span.children.map(child => <DrawSpan span={child} timeToScreen={timeToScreen} curHeightLevel={curHeightLevel + span.height_level + 1} setClickedSpan={setClickedSpan} />)}
        </div>
    )
}

function getWindowDimensions(): { windowWidth: number, windowHeight: number } {
    const { innerWidth, innerHeight } = window;
    return {
        windowWidth: innerWidth,
        windowHeight: innerHeight
    };
}

function useWindowDimensions(): { windowWidth: number, windowHeight: number } {
    const [windowDimensions, setWindowDimensions] = useState(
        getWindowDimensions()
    );

    useEffect(() => {
        function handleResize() {
            setWindowDimensions(getWindowDimensions());
        }

        window.addEventListener("resize", handleResize);
        return () => window.removeEventListener("resize", handleResize);
    }, []);

    return windowDimensions;
}

function countDisplayedSpans(nodeSpans: NodeSpans[]): number {
    let res = 0;
    for (let ns of nodeSpans) {
        for (let topLevelSpan of ns.spans) {
            res += countDisplayedSpansRec(topLevelSpan);
        }
    }
    return res;
}

function countDisplayedSpansRec(span: VisSpan): number {
    let res = 1;
    for (let child of span.children) {
        res += countDisplayedSpansRec(child);
    }
    return res;
}