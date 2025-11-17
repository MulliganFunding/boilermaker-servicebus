# Boilermaker API Reference

::: boilermaker.app.Boilermaker
  options:
    show_root_heading: true
    show_source: true
    heading_level: 2

## Message Lock Renewal

For long-running tasks that may exceed Azure Service Bus message lock duration, it's possible to use `renew_message_lock()` to maintain exclusive access to the message (and prevent it from being redelivered):

```py
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

!!! note "More information"
    Use message lock renewal for tasks that take longer than the message-lease duration for your queue.

    Consult the [Azure documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement#peeklock) for more information.