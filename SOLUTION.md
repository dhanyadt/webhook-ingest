# Solution

## What was broken

The main problems I found were duplicate webhook deliveries, inconsistent database updates and background work not being handled properly during shutdown.

A webhook could be received more than once. Without proper protection, the same event could be stored multiple times and the account statistics could also be increased multiple times.

I also found that the event, call information and account statistics were updated separately. If one of the operations failed, the database could end up only partially updated.

Recording processing was done in the background, but the application needed to wait for this work when shutting down. The stats cache also started empty after a restart even though the actual statistics were already stored in Postgres.

## How I handled duplicate webhooks

I used the `event_id` to identify duplicate webhook deliveries.

I added a unique constraint for `event_id` in Postgres and used `ON CONFLICT DO NOTHING` when inserting an event. This means that if the same webhook is received again, the database does not create another copy.

The event insert, call update, and account statistics update are also handled together in one database transaction. This helps keep these changes consistent if something goes wrong.

I chose Postgres for this because the event data is already stored there, so the database can directly enforce that the same event is not inserted twice.

## What I would change for 10,000 webhooks/sec

For a much larger number of webhooks, I would probably introduce a message queue between the HTTP server and the processing part.

The HTTP server could accept the webhook quickly and put it into the queue, while separate workers process the events.

I would also look at improving the database handling and making sure the application can process many requests at the same time without slowing down.

