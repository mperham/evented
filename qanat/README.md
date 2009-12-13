QANAT
======

Qanat is an SQS queue processor which works in an event-driven manner.  For this reason, it works
well for processing messages which require a lot of I/O, e.g. messages which require calling 3rd
party web services, scraping other web sites, etc.

The Ruby 1.8 and 1.9 VM implementations do not scale well.  Ruby 1.8 threads can't execute more than one at a time and on more than one core.  Many Ruby extensions are not thread-safe and Ruby itself places a GIL around many operations such that multiple Ruby threads can't execute concurrently.  JRuby is the only exception to this limitation currently but threaded code itself has issues - thread-safe code is notoriously difficult to write, debug and test.

Qanat will process up to N messages concurrently by using EventMachine to manage the overall processing.  Ruby 1.9 is required in order to take advantage of Fibers.

Author
--------

Mike Perham, @mperham, http://mikeperham.com