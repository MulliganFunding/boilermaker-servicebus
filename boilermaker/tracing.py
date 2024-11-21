""" tracing.py
    Provides opentelemetry tracing functionality such as context propagation

    ServiceBusReceivedMessage will have an `application_properties attribute like this:
    (Pdb) msg.application_properties
    {b'Diagnostic-Id': b'00-c2d9e6c2aef4196e5ec11fa3fb432873-0d65dd60358dfb94-01',
    b'traceparent': b'00-c2d9e6c2aef4196e5ec11fa3fb432873-0d65dd60358dfb94-01'}

    We need to pull out one of these and cast to a string to propagate context.
"""
from contextlib import asynccontextmanager

from azure.servicebus import ServiceBusReceivedMessage
from opentelemetry import trace
from opentelemetry.propagate import extract


def get_traceparent(event: ServiceBusReceivedMessage) -> str | None:
    if not hasattr(event, "application_properties"):
        return None

    if event.application_properties:
        if diagnostic_id := event.application_properties.get(b"traceparent", b""):
            return diagnostic_id.decode()  # type: ignore
    return None


def get_traceparent_context(event: ServiceBusReceivedMessage) -> dict[str, str]:
    if diagnostic_id := get_traceparent(event):
        return {"traceparent": diagnostic_id}
    return {}


@asynccontextmanager
async def start_span_from_parent_event_async(
    tracer: trace.Tracer,
    event: ServiceBusReceivedMessage,
    name: str,
    otel_enabled=False,
):
    """
    Wraps regular event_handler to add Parent Span information if otel enabled.
    """
    if otel_enabled:
        tracectx = extract(get_traceparent_context(event))
        with tracer.start_as_current_span(
            name, context=tracectx, kind=trace.SpanKind.CONSUMER
        ) as current_span:
            yield current_span
    else:
        yield None
