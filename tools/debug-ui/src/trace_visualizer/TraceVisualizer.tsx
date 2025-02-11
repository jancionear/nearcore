import React, { useMemo, useState, useEffect, useRef } from 'react';

//import json_data from './big_multi.json';// assert { type: "json" };
import { ExportTraceServiceRequest, Event, Resource, parseFromJson, KeyValue } from './otel';

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

type CameraData = {
    startTime: number,
    endTime: number,
    widthPerSecond: number,
    heightPerSpan: number,
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

    let camera: CameraData = {
        startTime: displayedSpansRange.startTime,
        endTime: displayedSpansRange.endTime,
        widthPerSecond: windowWidth / (displayedSpansRange.endTime - displayedSpansRange.startTime) * 1e9,
        heightPerSpan: 30,
    };

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
        <Timeline timelineHeight={timelineHeight} windowWidth={windowWidth} timelineState={timelineState} setTimelineState={setTimelineState} />
        <SearchBar searchBarHeight={searchBarHeight} />
        <DrawAllNodeSpans nodeSpans={nodeSpans} camera={camera} tracesHeight={tracesHeight} minStartTime={minStartTime} />
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

function Timeline({ timelineHeight, windowWidth, timelineState, setTimelineState }: { timelineHeight: number, windowWidth: number, timelineState: TimelineState, setTimelineState: (timelineState: TimelineState) => void }): JSX.Element {
    let selectedStartX = timeToScreen(timelineState.selectedStart, timelineState, windowWidth);
    let selectedEndX = timeToScreen(timelineState.selectedEnd, timelineState, windowWidth);
    let selectedWidth = selectedEndX - selectedStartX;

    let endBarWidth = 10;

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
        let clickedTime = screenToTime(e.clientX, timelineState, windowWidth);
        setTimelineState({ ...timelineState, action: TimelineAction.DraggingEdge, selectedStart: clickedTime, selectedEnd: clickedTime, deltaX: 0, notDraggedEdgeTime: clickedTime });
    };

    let timelineScrollHandler = (e: React.WheelEvent) => {
        // e.preventDefault(); Hard to make it work :/
        // See https://stackoverflow.com/questions/63663025/react-onwheel-handler-cant-preventdefault-because-its-a-passive-event-listener
        e.stopPropagation();

        let mouseX = e.clientX;
        let mouseTime = screenToTime(mouseX, timelineState, windowWidth);

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

    let middleBarWidth = Math.max(selectedWidth - endBarWidth, 0);
    let middleBarColor = "blue";
    let endBarsColor = "brown";

    return <div style={{ height: timelineHeight, position: "relative", border: "1px solid black", overflow: "hidden", background: "lightblue" }} onMouseDown={backgroundOnMouseDown} onWheel={timelineScrollHandler}>
        <TimePoints startTime={timelineState.startTime} endTime={timelineState.endTime} width={windowWidth} minStartTime={timelineState.minStartTime} />
        <div style={{ position: "absolute", left: selectedStartX + endBarWidth / 2, top: 0, width: middleBarWidth, height: timelineHeight, background: middleBarColor, opacity: 0.3 }} onMouseDown={middleBarOnMouseDown}
        > </div>
        <div style={{ position: "absolute", left: selectedStartX - endBarWidth / 2, top: 0, width: endBarWidth, height: timelineHeight, background: endBarsColor, opacity: 0.3 }} onMouseDown={leftBarOnMouseDown}></div>
        <div style={{ position: "absolute", left: selectedStartX + selectedWidth - endBarWidth / 2, top: 0, width: endBarWidth, height: timelineHeight, background: endBarsColor, opacity: 0.3 }} onMouseDown={rightBarOnMouseDown}></div>
    </div >
}

function TimePoints({ startTime, endTime, width, minStartTime }: { startTime: number, endTime: number, width: number, minStartTime: number }): JSX.Element {
    console.log("Generating time points from ", startTime, " to ", endTime, " with width ", width);

    let timePointWidth = 230;
    let numPoints = Math.floor(width / timePointWidth);
    let points = [];
    for (let i = 0; i < numPoints; i++) {
        let time = startTime + (endTime - startTime) / numPoints * i;
        let time_utc = new Date(time / 1e6).toISOString().replace("T", " ").replace("Z", "");
        let seconds_since_start = "" + ((time - minStartTime) / 1e9).toFixed(3) + "s";

        let screenPos = (time - startTime) / (endTime - startTime) * width;

        points.push(<div key={i} style={{ position: "absolute", left: screenPos, top: 0, width: timePointWidth, height: 20, borderLeft: "2px solid black" }}>
            <div>{time_utc}</div>
            <div>{seconds_since_start}</div>
        </div>);
    }

    console.log("Generated ", numPoints, " time points");

    return <div style={{ position: "relative", width: width, height: 20 }}>
        {points}
    </div>
}

function SearchBar({ searchBarHeight }: { searchBarHeight: number }): JSX.Element {
    return <div style={{ height: searchBarHeight, border: "1px solid black", background: "lightgrey" }}>
        This is the search bar (TODO)
    </div>
}

function timeToScreenSpace(time: number, camera: CameraData): number {
    return (time - camera.startTime) / 1e9 * camera.widthPerSecond;
}

function DrawAllNodeSpans({ nodeSpans, camera, tracesHeight, minStartTime }: { nodeSpans: NodeSpans[], camera: CameraData, tracesHeight: number, minStartTime: number }): JSX.Element {
    let width = camera.widthPerSecond * (camera.endTime - camera.startTime) / 1e9;

    return (<div style={{ height: tracesHeight, overflowX: "hidden", overflowY: "scroll" }}>
        <div style={{ height: 50, borderBottom: "1px solid black" }}> <TimePoints startTime={camera.startTime} endTime={camera.endTime} width={width} minStartTime={minStartTime} /> </div>
        <div>{nodeSpans.map(ns => <DrawNodeSpans nodeSpans={ns} camera={camera} />)} </div>
    </div>);
}

function DrawNodeSpans({ nodeSpans, camera }: { nodeSpans: NodeSpans, camera: CameraData }): JSX.Element {
    let width = timeToScreenSpace(camera.endTime, camera) - timeToScreenSpace(camera.startTime, camera);
    let height = nodeSpans.bbox.height * camera.heightPerSpan;
    return (<div>
        <div> Node: {nodeSpans.node.name} </div>
        <div style={{ position: "relative", border: "1px solid black", width: width, height: height }}>
            {nodeSpans.spans.map(s => <DrawSpan span={s} camera={camera} curHeightLevel={0} />)}
        </div>
    </div>

    )
}

function DrawSpan({ span, camera, curHeightLevel }: { span: VisSpan, camera: CameraData, curHeightLevel: number }): JSX.Element {
    let x = timeToScreenSpace(span.startTime, camera);
    let width = timeToScreenSpace(span.endTime, camera) - x;
    let y = (curHeightLevel + span.height_level) * camera.heightPerSpan;

    return (
        <>
            <div style={{ position: "absolute", left: x, top: y, height: camera.heightPerSpan, background: "white", color: "transparent", fontFamily: "monospace" }}>
                {span.name}
            </div>
            <div style={{ position: "absolute", left: x, top: y, width: width, height: camera.heightPerSpan, border: "1px solid orange", background: "yellow", }}></div>
            <div style={{ position: "absolute", left: x, top: y, fontFamily: "monospace" }}>
                {span.name}
            </div >
            {span.children.map(child => <DrawSpan span={child} camera={camera} curHeightLevel={curHeightLevel + span.height_level + 1} />)}
        </>
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