# Boilermaker API Reference

::: boilermaker.app.Boilermaker
    options:
      show_root_heading: true
      show_source: true
      heading_level: 2

## Message Lock Renewal

For long-running tasks that may exceed Azure Service Bus message lock duration, use `renew_message_lock()` to maintain exclusive access to the message:

```python
@app.task()
async def process_large_file(state, file_path: str):
    """Process a large file with periodic lock renewal."""

    lines = await read_file_lines(file_path)

    for i, line in enumerate(lines):
        await process_line(line)

        # Renew message lock every 50 lines
        if i % 50 == 0:
            await state.app.renew_message_lock()

    return f"Processed {len(lines)} lines"
```

!!! note "When to Use"
    Use message lock renewal for tasks that:

    - Process large datasets or files
    - Make multiple external API calls
    - Perform complex computations taking several minutes
    - Cannot be easily broken into smaller tasks

!!! warning "Lock Duration Limits"
    Azure Service Bus has maximum lock duration limits. For extremely long operations, consider breaking them into smaller, chainable tasks instead.