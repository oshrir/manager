Distributed PDF Converter
=========================
Stav Faran - 308096270, Oshri Rozenberg - 204354344

Running instructions:
--------------------
TODO

Security:
---------
We still need to understand how to transfer the credentials to the local app (or we doesnt have to?), but besides that - the manager and all of the workers get temporary credentials from their IAM role - which means we don't transfer them credentials at all, particulary not as plain text.

Scalability:
------------
The thread pool enables the manager to deal with a big amount of clients that are running at the same time.
TODO - add more.
~~Did you think about scalability? Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion? Scalability is very important aspect of the system, be sure it is scalable!~~

Persistence:
------------
What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!

Threads:
--------
We used threads in our Manager - one thread which operates the thread pool for the clients, and another thread that processes the responses from the workers. This is the only place where we thought it is neccessary to use threads in out application, so we could handle a big amount of clients at the same time.




We ran at least 5 client at the same time, the all worked properly, finished properly and the results are correct.

The termination process is well managed, and everything is closed once requested - local app, manager, workers and queues are deleted.


Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.



Did you take in mind the system limitations that we are using? Be sure to use it to its fullest!


Are all your workers working hard? Or some are slacking? Why?


Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!


Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?


All of this need to be explained properly and added to your README file. In addition to the requirements above.
