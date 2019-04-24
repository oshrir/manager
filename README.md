Distributed PDF Converter
=========================
Stav Faran - 308096270, Oshri Rozenberg - 204354344

Description:
-----------
This application takes a text file loaded with actions to perform on PDF files (stored all over the internet), ...
TODO - add more info about how the program works

Running instructions:
--------------------
1. Store your AWS credentials in `~/.aws/credentials`
```
   [default]
   aws_access_key_id= ???
   aws_secret_access_key= ???
```
2. run using `java -jar PdfConverter.jar inputFileName outputFileName n (terminate)` (terminate is optional)

Security:
---------
The manager and all of the workers get temporary credentials from their IAM role. Therefore, we don't transfer them credentials at all, particulary not as plain text.

Scalability:
------------
We ran a few applications at a time to test the program, they all worked properly, finished properly and the results were correct.
While checking our implementation, we noticed that there is a built-in limitation in AWS EC2 regarding the maximum amount of instances we are able to run - maximum of 20 instances at the same time. According to that, we added this limitation to our implementation, so the application won't crush in case that the manager tries to create instances to a total amount of 20 or more, with the given limitations the program will be able to perform on large amount of clients thanks to a thread pool mechanism we used in the manager.

Persistence:
------------
If one of the workers is impaired we implemented a fail mechanism that uses the SQS time-out functionallity to resend the message into the input queue of the workers and activated a reboot function so that a new worker will replace the impaired one, morever if the worker encounters an exception while working it will send the exception to the worker and regroup to handle a new message, In case of broken communication we created a fail-safe mechanism that tries to send the SQS message and if failed will try again until succession.

Threads:
--------
We used threads in our Manager - one thread which operates the thread pool for the clients, and another thread that processes the responses from the workers. This is the only place where we thought it is neccessary to use threads in out application, so we could handle a big amount of clients at the same time.

Termination:
--------
The termination process is well managed, and everything is closed once requested - local app, manager, workers and queues are deleted.

System:
--------
All the workers are working equally, they have access to the input queue where they fetch an assignment, work on it, and send the resulting work back to the output queue, they know nothing more than they need and they are treated equally, The Manager is resposible of assembling the data and distribute it.
