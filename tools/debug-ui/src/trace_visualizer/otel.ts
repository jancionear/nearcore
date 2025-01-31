export type ExportTraceServiceRequest = {
    resourceSpans: ResourceSpans[];
}

export type ResourceSpans = {
    resource: Resource;
    scopeSpans: ScopeSpans[];
    // schema_url: string;
}


export type Resource = {
    attributes: Attributes;
    // dropped_attributes_count: number,
}

export type ScopeSpans = {
    // scope: InstrumentationScope;
    spans: Span[];
    // schema_url
}

export type Span = {
    traceId: string
    spanId: string
    // trace_state
    parentSpanId: string,
    // flags
    name: string,
    // kind
    startTimeUnixNano: number,
    endTimeUnixNano: number,
    attributes: Attributes,
    // droppedAttributesCount: number,
    events: Event[],
    // droppedEventsCount: number,
    links: Link[],
    // droppedLinksCount: number,
    // status: Status,
}

export type Link = {
    traceId: string,
    spanId: string,
    attributes: Attributes
    // droppedAttributesCount: number,
    // flags
}

export type Attributes = {
    attribute_map: Map<string, AttributeValue>
}

export type AttributeValue = {
    stringValue: string,
    intValue: number,
    boolValue: boolean,
    doubleValue: number,
    // arrayValue:
    // kvlistValue:
    // bytesValue
}

export type Event = {
    timeUnixNano: number,
    name: string,
    attributes: Attributes,
    // droppedAttributesCount: number,
}

export function parseFromJson(json_data: any): ExportTraceServiceRequest[] {
    // TODO - do proper parsing and error detection
    return json_data as ExportTraceServiceRequest[];
}