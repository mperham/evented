EventedMagick
----------------

A EventMachine-aware wrapper for ImageMagick command line.  Uses EM.system to execute if available.
Requires Ruby 1.9 since it uses Fibers.  The internals have also been rewritten to reduce the number
of system() calls.  These changes together reduced the time required to run my test from 20sec to 5sec
versus the stock MiniMagick library.

Thanks
==========

Based on mini_magick by http://github.com/probablycorey

Author
==========

Mike Perham, mperham AT gmail.com
http://github.com/mperham
http://twitter.com/mperham
http://mikeperham.com

