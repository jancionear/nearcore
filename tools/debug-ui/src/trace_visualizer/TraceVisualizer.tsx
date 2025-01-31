import React, { useMemo, useState } from 'react';

import json_data from './big.json';// assert { type: "json" };
import { ExportTraceServiceRequest, parseFromJson } from './otel';

type SimpleSpan = {
    name: string,
    startTimeUnixNano: number,
    endTimeUnixNano: number,
}

function extractSimpleSpans(otel_requests: ExportTraceServiceRequest[]): SimpleSpan[] {
    let spans: SimpleSpan[] = [];
    for (let request of otel_requests) {
        //console.log("request: ", request);
        for (let resource_spans of request.resourceSpans) {
            for (let scope_spans of resource_spans.scopeSpans) {
                for (let span of scope_spans.spans) {
                    spans.push({
                        name: span.name,
                        startTimeUnixNano: span.startTimeUnixNano,
                        endTimeUnixNano: span.endTimeUnixNano,
                    });
                }
            }
        }
    }
    return spans;
}

type Span2 = {
    spanId: string,
    name: string,
    startTime: number,
    endTime: number,
    parentSpanId: string,
    children: Span2[],
}

function extractTopLevelSpans(otel_requests: ExportTraceServiceRequest[]): Span2[] {
    let spans: Span2[] = [];
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
                    });
                }
            }
        }
    }

    // Map from spanId to span
    let spanIdToSpan: { [spanId: string]: Span2 } = {};
    for (let span of spans) {
        spanIdToSpan[span.spanId] = span;
    }

    for (let span of spans) {
        if (span.parentSpanId in spanIdToSpan) {
            spanIdToSpan[span.parentSpanId].children.push(span);
        }
    }

    // Spans whose parent_id is null
    let top_level_spans: Span2[] = [];
    for (let span of spans) {
        if (span.parentSpanId === "") {
            top_level_spans.push(span);
        }
    }
    return top_level_spans;
}

function getSpanDepth(span: Span2): number {
    if (span.children.length === 0) {
        return 1;
    }
    let maxChildDepth = 0;
    for (let child of span.children) {
        maxChildDepth = Math.max(maxChildDepth, getSpanDepth(child));
    }
    return 1 + maxChildDepth;
}

type VisualSpan = {
    span: Span2,
    width: number,
    height: number,
    x: number,
    y: number,
}

function makeVisualSpan(span: Span2, widthPerSecond: number, spanHeight: number, startTime: number): VisualSpan {
    return {
        span: span,
        width: (span.endTime - span.startTime) / 1e9 * widthPerSecond,
        height: spanHeight,
        x: (span.startTime - startTime) / 1e9 * widthPerSecond,
        y: 0,
    };
}

function getChildVisualSpans(parentSpan: VisualSpan, widthPerSecond: number, spanHeight: number, startTime: number): VisualSpan[] {
    let visualSpans: VisualSpan[] = [];
    for (let child of parentSpan.span.children) {
        let visualSpan = makeVisualSpan(child, widthPerSecond, spanHeight, startTime);
        visualSpan.y = parentSpan.y + spanHeight;
        let subChildVisualSpans = getChildVisualSpans(visualSpan, widthPerSecond, spanHeight, startTime);
        visualSpans.push(visualSpan);
        visualSpans.push(...subChildVisualSpans);
    }
    return visualSpans;
}

function isBetween(coord: number, start: number, end: number): boolean {
    return coord >= start && coord <= end;
}

function isCollidingInAxis(coord1: number, size1: number, coord2: number, size2: number): boolean {
    return isBetween(coord1, coord2, coord2 + size2) || isBetween(coord1 + size1, coord2, coord2 + size2) || isBetween(coord2, coord1, coord1 + size1) || isBetween(coord2 + size2, coord1, coord1 + size1);
}

function isColliding(span1: VisualSpan, span2: VisualSpan): boolean {
    return isCollidingInAxis(span1.y, span1.height, span2.y, span2.height) && isCollidingInAxis(span1.x, span1.width, span2.x, span2.width);
}

export const TraceVisualizer = () => {
    let parsed_data = parseFromJson(json_data);
    //console.log(parsed_data);
    let simple_spans = extractSimpleSpans(parsed_data);
    console.log("simple spans: ", simple_spans);

    let minStartTime = Math.min(...simple_spans.map(span => span.startTimeUnixNano));
    let maxEndTime = Math.max(...simple_spans.map(span => span.endTimeUnixNano));
    console.log("number of spans: ", simple_spans.length);
    console.log("minStartTime: ", minStartTime)
    console.log("maxEndTime: ", maxEndTime)
    console.log("time_secs: ", (maxEndTime - minStartTime) / 1e9);

    let topLevelSpans = extractTopLevelSpans(parsed_data);
    console.log("topLevelSpans: ", topLevelSpans);

    let maxDepth = Math.max(...topLevelSpans.map(span => getSpanDepth(span)));
    console.log("maxDepth: ", maxDepth);

    let sortedByStartTime = topLevelSpans.slice().sort((a, b) => a.startTime - b.startTime);

    let visualSpans: VisualSpan[] = [];
    const widthPerSecond = 40000;
    const heightPerSpan = 50;
    const startTime = minStartTime;

    for (let span of sortedByStartTime) {
        let width = (span.endTime - span.startTime) / 1e9 * widthPerSecond;
        let visualSpan = makeVisualSpan(span, widthPerSecond, heightPerSpan, startTime);
        visualSpan.height = getSpanDepth(span) * heightPerSpan;

        let collidingSpans = visualSpans.filter(vs => isColliding(visualSpan, vs));
        while (collidingSpans.length > 0) {
            visualSpan.y += heightPerSpan;
            collidingSpans = visualSpans.filter(vs => isColliding(visualSpan, vs));
        }

        visualSpans.push(visualSpan);
    }

    let child_spans: VisualSpan[] = [];
    for (let visualSpan of visualSpans) {
        let childVisualSpans = getChildVisualSpans(visualSpan, widthPerSecond, heightPerSpan, startTime);
        child_spans.push(...childVisualSpans);
    }

    visualSpans.push(...child_spans);

    for (let vSpan of visualSpans) {
        vSpan.height = heightPerSpan;
    }

    /// For every visualSpan, draw a div whose top left corner is at x, y and has given width and height
    return (
        <div>
            <div style={{ display: "flex", flexDirection: "column" }}>
                {visualSpans.map(visualSpan => {
                    return <div key={visualSpan.span.spanId} style={{ position: "absolute", left: visualSpan.x, top: visualSpan.y, width: visualSpan.width, height: visualSpan.height, border: "1px solid black", overflow: "hidden", background: "cornsilk" }}>
                        {visualSpan.span.name}
                    </div>
                })}
            </div>
        </div>
    );
}

