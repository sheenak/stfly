import sys
import json
import re
from datetime import datetime, timedelta
from dateutil import rrule
import datetime
import csv
import heapq
import logging
logger = logging.getLogger('__name__')
handler = logging.FileHandler('event_processing.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)

# Change this variable depending on how many of the top LTV customers you would like in the output
top_n_ltv_customers = 10


class EventStaging(object):
    """
    Data structure to store all events in the input file
    """
    events = {}

    def append_event(self, event):
        if event.event_type in self.events:
            self.events[event.event_type].append(event)
        else:
            self.events[event.event_type] = [event]


class Event(object):
    """
    Parent class with shared event fields
    """
    def __init__(self, event_type, verb, customer_id, event_time, key):
        self.event_type = event_type
        self.verb = verb
        self.key = key
        self.customer_id = customer_id
        self.event_time = event_time


class CustomerEvent(Event):
    """
    Derived class with fields specific to the customer event
    """
    def __init__(self, event_type, verb, customer_id, event_time, key, last_name, adr_city, adr_state):
        Event.__init__(self, event_type, verb, customer_id, event_time, key)
        self.last_name = last_name
        self.adr_city = adr_city
        self.adr_state = adr_state


class SiteVisitEvent(Event):
    """
    Derived class with fields specific to the site visit event
    """
    def __init__(self, event_type, verb, customer_id, event_time, key, tags):
        Event.__init__(self, event_type, verb, customer_id, event_time, key)
        self.tags = tags


class ImageUploadEvent(Event):
    """
    Derived class with fields specific to the image upload event
    """
    def __init__(self, event_type, verb, customer_id, event_time, key, camera_make, camera_model):
        Event.__init__(self, event_type, verb, customer_id, event_time, key)
        self.camera_make = camera_make
        self.camera_model = camera_model


class OrderEvent(Event):
    """
    Derived class with fields specific to the order event
    """
    def __init__(self, event_type, verb, customer_id, event_time, key, total_amount):
        Event.__init__(self, event_type, verb, customer_id, event_time, key)
        self.total_amount = total_amount



#####################################################################################################


def get_weeks_between_dates(t1, t2):
    """

    Given a start and end date, return the number of weeks between the two
    :param t1: Start timestamp of events timeframe
    :param t2: End timestamp of events timeframe
    :return: Number of weeks between the timestamps
    """
    start_time = datetime.datetime.strptime(t1, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_time = datetime.datetime.strptime(t2, "%Y-%m-%dT%H:%M:%S.%fZ")
    weeks = rrule.rrule(rrule.WEEKLY, dtstart=start_time, until=end_time)

    return weeks.count()



def create_customer_summary(events_data, customer_summary):
    """

    Create an in-memory summary for each customer.
    Total order value, total number of site visits, revenue/visit, visits/week, LTV, first customer visit timestamp,
    last customer visit timestamp are stored against each customer_id.
    :param events_data: Data Structure that stores all the events
    :param customer_summary: Dictionary to store customer summary with customer_id as the key
    """
    timeframe_start = None
    timeframe_end = None
    for event_type, events_list in events_data.events.iteritems():
        for e in events_list:

            # Start and end of the timeframe of the event data set.
            if (e.event_time < timeframe_start or not timeframe_start):
                timeframe_start = e.event_time
            if (e.event_time > timeframe_end or not timeframe_end):
                timeframe_end = e.event_time

            # Initialize customer attributes when first encountered in the dataset
            if e.customer_id not in customer_summary:
                customer_summary[e.customer_id] = {'customer_id':e.customer_id, 'total_visits':0,
                                                   'total_order_value':0,'first_seen':e.event_time,
                                                   'last_seen':e.event_time, 'revenue_per_visit':0,
                                                   'visits_per_week':0, 'LTV':0,
                                                   'orders':{}}

            # If the event type is CUSTOMER, then update registration time and name
            if e.event_type == 'CUSTOMER':
                customer_summary[e.customer_id]['last_name'] = e.last_name
                if e.verb == 'NEW':
                    customer_summary[e.customer_id]['registration_time'] = e.event_time
            # If the event type is SITE VISIT then increment the number of total visits by 1
            elif e.event_type == 'SITE_VISIT':
                customer_summary[e.customer_id]['total_visits'] += 1
            # If the event type is ORDER then add the order value to the total order value of the customer
            elif e.event_type == 'ORDER':
                amount = re.findall('\d+\.\d+', e.total_amount)[0]
                if e.key in customer_summary[e.customer_id]['orders']:
                    if e.event_time > customer_summary[e.customer_id]['orders'][e.key]['last_updated_time']:
                        customer_summary[e.customer_id]['orders'][e.key]['order_value'] = float(amount)
                        customer_summary[e.customer_id]['orders'][e.key]['last_updated_time'] = e.event_time
                else:
                    customer_summary[e.customer_id]['orders'][e.key] = {'order_value':float(amount),
                                                                        'last_updated_time':e.event_time}
                #customer_summary[e.customer_id]['total_order_value'] += float(amount[0])

            # Update the first and last timestamps that the customer was seen on the website
            if e.event_time < customer_summary[e.customer_id]['first_seen']:
                customer_summary[e.customer_id]['first_seen'] = e.event_time
            if e.event_time > customer_summary[e.customer_id]['last_seen']:
                customer_summary[e.customer_id]['last_seen'] = e.event_time

    # Once all the data has been iterated through and the customer summary is created, calculate
    # revenue/visit, visits/week and LTV
    for cust_id, summary in customer_summary.iteritems():
        for order_id, order_details in summary['orders'].iteritems():
                customer_summary[cust_id]['total_order_value'] += float(order_details['order_value'])
        if summary['total_visits'] > 0:
            customer_summary[cust_id]['revenue_per_visit'] = float(summary['total_order_value'])/summary['total_visits']
        number_of_weeks = get_weeks_between_dates(summary['first_seen'], timeframe_end)
        customer_summary[cust_id]['visits_per_week'] = float(summary['total_visits'])/number_of_weeks
        customer_summary[cust_id]['LTV'] = calc_LTV(customer_summary[cust_id]['revenue_per_visit'],
                                                    customer_summary[cust_id]['visits_per_week'])


def calc_LTV(revenue_per_visit, visits_per_week):
    """
    Calculates simple LTV of a customer, given revenue/visit and visits/week
    :param revenue_per_visit: Total order value per visit
    :param visits_per_week: Total visits per week
    :return: Simple LTV
    """
    average_customer_lifespan = 10
    return round(52 * revenue_per_visit * visits_per_week * average_customer_lifespan, 2)


def write_to_file(filename, mode, row):
    """
    Write the data passed in a list format to a csv file
    :param filename: Name of the file
    :param mode: Mode to open the file
    :param row: Data to be written to the file
    """
    try:

        f = open(filename, mode)
        w = csv.writer(f, delimiter = ',')
        w.writerow(row)

    except:
        logger.exception("Errror writing output to file")


def top_n_simple_ltv_customers(top_n_ltv_customers, events_data):
    """
    Writes the top n customers with the highest LTV to an output file
    :param top_n_ltv_customers: Number of highest LTV customers
    :param events_data: Data Structure that stores all the events
    """
    customer_summary = {}
    output_filename = '../output/output.txt'
    # Creates a summary for each customer that includes the simple LTV
    create_customer_summary(events_data, customer_summary)

    # Using the heap queue data structure to sort the customers based on their LTV and return the top n customers

    try:
        # Header of the output file
        write_to_file(output_filename,'w',['Customer_ID', 'Last_Name', 'LTV'])

        top_n_customer_summary = heapq.nlargest(top_n_ltv_customers, customer_summary.values(), key=lambda x:x['LTV'])

        # Iterate through the top n customer data returned and write to the output file
        for c in top_n_customer_summary:
            write_to_file(output_filename,'a',[c['customer_id'], c['last_name'], c['LTV']])
    except:
        logger.exception("Error calculating LTV.")
        raise




def is_duplicate_event(event, events_data):
    """

    If an event with the same key has already been parsed, return False, so the duplicate event can be excluded.
    The present dedup algorithm is based on the event key alone, which may not be unique when a Customer or Order event
    update is done. If the verb is UPDATE for these two event types, the function returns False.
    Another approach for de-dup would be hashing the events while ingesting them.
    :param event: Event object
    :param events_data: Data Structure that stores all the events
    :return: boolean to indicate whether an event is a duplicate or not
    """

    try:
        events_list = events_data.events[event.event_type]
    except KeyError:
        return False


    for e in events_list:
        if event.key == e.key:
            if (event.event_type == 'CUSTOMER' or event.event_type == 'ORDER'):
                if event.verb == 'NEW':
                    return True
                elif event.verb == 'UPDATE':
                    return False
            else:
                return True
    return False



# Load the event string as json and return it to the calling function
def ingest(json_string, events_data):
    """
    Ingest the input data and add to the events data structure
    :param json_string: Event in JSON format
    :param events_data: Data Structure that stores all the events
    """

    try:
        e = json.loads(json_string)
        type = e['type']
        if type == 'CUSTOMER':
            event = CustomerEvent(e['type'], e['verb'], e['key'],e['event_time'],
                                  e['key'],e['last_name'], e['adr_city'], e['adr_state'])

        elif type == 'SITE_VISIT':
            event = SiteVisitEvent(e['type'], e['verb'], e['customer_id'],e['event_time'], e['key'], e['tags'])
        elif type == 'IMAGE':
            event = ImageUploadEvent(e['type'], e['verb'], e['customer_id'],e['event_time'],
                                    e['key'], e['camera_make'], e['camera_model'])
        elif type == 'ORDER':
            event = OrderEvent(e['type'], e['verb'], e['customer_id'],e['event_time'], e['key'], e['total_amount'])
        else:
            logger.error("Undefined event type: {}".format(e))

        # Skip the event if it's a duplicate
        if is_duplicate_event(event, events_data):
            logger.error("Duplicate Event: {}".format(e))
            return

        events_data.append_event(event)

    except:
        # Log corrupt data in the input file
        logger.exception("Invalid event format")


if __name__ == '__main__':
    #Data structure to load input file events
    events_data = EventStaging()
    try:
        #File name passed are argument to the script
        input_file = sys.argv[1]
        #Read the input file events
        with open(input_file, 'r') as l:
            for line in l:
                #Store the events in memory
                ingest(line, events_data)

        top_n_simple_ltv_customers(top_n_ltv_customers, events_data)

    except:
        logger.exception("Errror reading input")
        raise

