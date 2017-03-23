How to run the program:
-----------------------

Example: python sfly_event_processing.py input.txt

The events filename should be passed as a parameter to the script.
The script will process the events in input.txt and calculate the LTV for each customer and will generate a file
output.txt with the 10 highest LTV customers.
The global variable top_n_ltv_customers has been set to 10 to return the top 10 LTV customers. 

The script has 2 main functions: ingest and top_n_simple_ltv_customers

ingest function:
----------------
	It takes an input event line, converts it to the respective event object type. It also stores all the events in a
	 data structue called EventStaging. The function also takes care of limited de-duplication of events.*
	All the data is retained in the data structure for multiple analytic functions apart from the immediate requirement
	 of calculating simple LTV.

top_n_simple_ltv_customers function:
------------------------------------
	It creates a summary for each customer and calculates and creates an output.txt file with the N highest LTV
	 customers. The customer_id, last_name and LTV are the fields included in the output.


Assumptions:
	* Timeframe for the LTV calculation is determined as the weeks spanned by the earliest timestamp for a specific
	 customer and the latest timestamped event in the input file.
	 
	* The de-duplication of events is done by the event key. The exception is only for customer and order event updates.
	 These events are not considered as duplicates even when they have the same key.
	 
	* If an update event for an order is present, the total amount for the last update event is assumed to be the total
	 value of that order.
   
