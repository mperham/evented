QANAT
======

Qanat is an SQS queue processor which works in an event-driven manner.  For this reason, it works
well for processing messages which require a lot of I/O, e.g. messages which require calling 3rd
party web services, scraping other web sites, etc.

Most Ruby implementations do not scale well.  Many Ruby extensions are not thread-safe and Ruby itself
places a GIL around many operations such that multiple Ruby threads can't execute concurrently.  JRuby
is the only exception to this limitation currently but threaded code itself has issues - thread-safe code 
is notoriously difficult to write, debug and test.

Qanat will process up to 10 messages concurrently and try to keep 10 ready for processing so that the
process is never sitting idle unless there are no messages to actually process.

It does this by reserving a "thread" for managing an internal queue which holds messages to process.
That thread checks the status of the internal queue once per second to ensure it always has at least 
5 messages.