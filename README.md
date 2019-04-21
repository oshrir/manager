Distributed PDF Converter
=========================
Stav Faran - 308096270, Oshri Rozenberg - 204354344

Description:
-----------
This application takes a text file loaded with actions to perform on PDF files (stored all over the internet), ...

Running instructions:
--------------------
1. Store your AWS credentials in `~/.aws/credentials`
```
   [default]
   aws_access_key_id= ???
   aws_secret_access_key= ???
```
2. blah blah blah
3. blah blah blah
4. run using `java -jar PdfConverter.jar inputFileName outputFileName n (terminate)` (terminate is optional)

Security:
---------
The manager and all of the workers get temporary credentials from their IAM role. Therefore, we don't transfer them credentials at all, particulary not as plain text.

Scalability:
------------
The thread pool enables the manager to deal with a big amount of clients that are running at the same time.
TODO - add more.
~~Did you think about scalability? Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion? Scalability is very important aspect of the system, be sure it is scalable!~~
We ran at least 5 client at the same time, the all worked properly, finished properly and the results are correct.
While checking our implementation, we noticed that there is a built-in limitation in AWS EC2 regarding the maximum amount of instances we are able to run - maximum of 20 instances at the same time. According to that, we added this limitation to our implementation, so the application won't crush in a case that the manager tries to create instances to a total amount of 20 or more.

Persistence:
------------
What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!

Threads:
--------
We used threads in our Manager - one thread which operates the thread pool for the clients, and another thread that processes the responses from the workers. This is the only place where we thought it is neccessary to use threads in out application, so we could handle a big amount of clients at the same time.






The termination process is well managed, and everything is closed once requested - local app, manager, workers and queues are deleted.


Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.






Are all your workers working hard? Or some are slacking? Why?


Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!


Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?


All of this need to be explained properly and added to your README file. In addition to the requirements above.
