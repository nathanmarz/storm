---
title: Storm and Kestrel
layout: documentation
documentation: true
---
This page explains how to use Storm to consume items from a Kestrel cluster.

## Preliminaries
### Storm
This tutorial uses examples from the [storm-kestrel](https://github.com/nathanmarz/storm-kestrel) project and the [storm-starter](http://github.com/apache/storm/blob/{{page.version}}/examples/storm-starter) project. It's recommended that you clone those projects and follow along with the examples. Read [Setting up development environment](Setting-up-development-environment.html) and [Creating a new Storm project](Creating-a-new-Storm-project.html) to get your machine set up.
### Kestrel
It assumes you are able to run locally a Kestrel server as described [here](https://github.com/nathanmarz/storm-kestrel).

## Kestrel Server and Queue
A single kestrel server has a set of queues. A Kestrel queue is a very simple message queue that runs on the JVM and uses the memcache protocol (with some extensions) to talk to clients. For details, look at the implementation of the [KestrelThriftClient](https://github.com/nathanmarz/storm-kestrel/blob/master/src/jvm/org/apache/storm/spout/KestrelThriftClient.java) class provided in [storm-kestrel](https://github.com/nathanmarz/storm-kestrel) project.

Each queue is strictly ordered following the FIFO (first in, first out) principle. To keep up with performance items are cached in system memory; though, only the first 128MB is kept in memory. When stopping the server, the queue state is stored in a journal file.

Further, details can be found [here](https://github.com/nathanmarz/kestrel/blob/master/docs/guide.md).

Kestrel is:
* fast
* small
* durable
* reliable

For instance, Twitter uses Kestrel as the backbone of its messaging infrastructure as described [here] (http://bhavin.directi.com/notes-on-kestrel-the-open-source-twitter-queue/).

## Add items to Kestrel
At first, we need to have a program that can add items to a Kestrel queue. The following method takes benefit of the KestrelClient implementation in [storm-kestrel](https://github.com/nathanmarz/storm-kestrel). It adds sentences into a Kestrel queue randomly chosen out of an array that holds five possible sentences.

```
    private static void queueSentenceItems(KestrelClient kestrelClient, String queueName)
			throws ParseError, IOException {

		String[] sentences = new String[] {
	            "the cow jumped over the moon",
	            "an apple a day keeps the doctor away",
	            "four score and seven years ago",
	            "snow white and the seven dwarfs",
	            "i am at two with nature"};

		Random _rand = new Random();

		for(int i=1; i<=10; i++){

			String sentence = sentences[_rand.nextInt(sentences.length)];

			String val = "ID " + i + " " + sentence;

			boolean queueSucess = kestrelClient.queue(queueName, val);

			System.out.println("queueSucess=" +queueSucess+ " [" + val +"]");
		}
	}
```

## Remove items from Kestrel

This method dequeues items from a queue without removing them.
```
    private static void dequeueItems(KestrelClient kestrelClient, String queueName) throws IOException, ParseError
			 {
		for(int i=1; i<=12; i++){

			Item item = kestrelClient.dequeue(queueName);

			if(item==null){
				System.out.println("The queue (" + queueName + ") contains no items.");
			}
			else
			{
				byte[] data = item._data;

				String receivedVal = new String(data);

				System.out.println("receivedItem=" + receivedVal);
			}
		}
```

This method dequeues items from a queue and then removes them.
```
    private static void dequeueAndRemoveItems(KestrelClient kestrelClient, String queueName)
    throws IOException, ParseError
		 {
			for(int i=1; i<=12; i++){

				Item item = kestrelClient.dequeue(queueName);


				if(item==null){
					System.out.println("The queue (" + queueName + ") contains no items.");
				}
				else
				{
					int itemID = item._id;


					byte[] data = item._data;

					String receivedVal = new String(data);

					kestrelClient.ack(queueName, itemID);

					System.out.println("receivedItem=" + receivedVal);
				}
			}
	}
```

## Add Items continuously to Kestrel

This is our final program to run in order to add continuously sentence items to a queue called **sentence_queue** of a locally running Kestrel server.

In order to stop it type a closing bracket char ']' in console and hit 'Enter'.

```
    import java.io.IOException;
    import java.io.InputStream;
    import java.util.Random;

    import org.apache.storm.spout.KestrelClient;
    import org.apache.storm.spout.KestrelClient.Item;
    import org.apache.storm.spout.KestrelClient.ParseError;

    public class AddSentenceItemsToKestrel {

    	/**
    	 * @param args
    	 */
    	public static void main(String[] args) {

    		InputStream is = System.in;

			char closing_bracket = ']';

			int val = closing_bracket;

			boolean aux = true;

			try {

				KestrelClient kestrelClient = null;
				String queueName = "sentence_queue";

				while(aux){

					kestrelClient = new KestrelClient("localhost",22133);

					queueSentenceItems(kestrelClient, queueName);

					kestrelClient.close();

					Thread.sleep(1000);

					if(is.available()>0){
					 if(val==is.read())
						 aux=false;
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (ParseError e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			System.out.println("end");

	    }
	}
```
## Using KestrelSpout

This topology reads sentences off of a Kestrel queue using KestrelSpout, splits the sentences into its constituent words (Bolt: SplitSentence), and then emits for each word the number of times it has seen that word before (Bolt: WordCount). How data is processed is described in detail in [Guaranteeing message processing](Guaranteeing-message-processing.html).

```
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("sentences", new KestrelSpout("localhost",22133,"sentence_queue",new StringScheme()));
    builder.setBolt("split", new SplitSentence(), 10)
    	        .shuffleGrouping("sentences");
    builder.setBolt("count", new WordCount(), 20)
	        .fieldsGrouping("split", new Fields("word"));
```

## Execution

At first, start your local kestrel server in production or development mode.

Than, wait about 5 seconds in order to avoid a ConnectException.

Now execute the program to add items to the queue and launch the Storm topology. The order in which you launch the programs is of no importance.

If you run the topology with TOPOLOGY_DEBUG you should see tuples being emitted in the topology.
