QANAT
======

"Scalable AWS processing for Ruby"

Qanat is one of the few words recognized by Scrabble that begin with Q and don't require a U following.

Context
---------

The Ruby 1.8 and 1.9 VM implementations do not scale well.  Ruby 1.8 threads can't execute more than one at a time and on more than one core.  Many Ruby extensions are not thread-safe and Ruby itself places a GIL around many operations such that multiple Ruby threads can't execute concurrently.  JRuby is the only exception to this limitation currently but threaded code itself has issues - thread-safe code is notoriously difficult to write, debug and test. 

At my current employer, we use S3, SQS and SimpleDB to store data.  Those services scale very well to huge volumes of data but don't have incredible response times so when you write code which grabs a message from a queue, performs a SimpleDB lookup, makes a change and stores some other data to S3, that entire process might take 2 seconds, where 0.1 sec is actually spent performing calculations with the CPU and the other 1.9 seconds is spent blocked, doing nothing and waiting for the various Amazon web services to respond.

Qanat is an SQS queue processor which uses an event-driven architecture to work around these issues.  It works well for processing messages which spend a lot of time performing I/O, e.g. messages which require calling 3rd party web services, scraping other web sites, making long database queries, etc.


Design
-------

Qanat will process up to N messages concurrently, using EventMachine to manage the overall processing.  Ruby 1.9 is required.

Qanat provides basic implementations of SQS, SimpleDB and S3 event-based clients.  These clients can be used in your own message processing code.

Install
---------

    gem install qanat

You will need to put a file with your Amazon credentials in either `~/.qanat.amzn.yml` or `QANAT_ROOT/config/amzn.yml`.  The contents should look like this:

    defaults: &defaults
      access_key: <your AWS access key>
      secret_key: <your AWS secret key>
      timeout: 5

    development:
      <<: *defaults

    test:
      <<: *defaults

    production:
      <<: *defaults



Author
--------

Mike Perham, @mperham, http://mikeperham.com