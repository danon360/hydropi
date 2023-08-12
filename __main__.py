
import getopt, sys, os
import hydroPI
import json
import triggers

options = "hf:s:top"
 
# Long options
long_options = ["Help", "addresses-file=", "addresses-str=", "trigger-alert", "get-outages", "get-planned-interuptions"]


addresses_file = "addresses.json"
addresses_str = ""
trigger_alert = False
pts_str = ""
get_outages = False
get_planned_interuptions = False


def process_args():


    global addresses_file
    global addresses_str
    global trigger_alert
    global get_outages
    global get_planned_interuptions
    

    argumentList = sys.argv[1:]
    try:
        # Parsing argument
        arguments, values = getopt.getopt(argumentList, options, long_options)
        
        # checking each argument
        for currentArgument, currentValue in arguments:
    
            if currentArgument in ("-h", "--Help"):
                print ("Displaying Help")
                
            elif currentArgument in ("-f", "--addresses-file"):
                addresses_file = currentValue
                
            elif currentArgument in ("-s", "--addresses-str"):
                addresses_str = currentValue
            
            elif currentArgument in ("-f", "--trigger-alert"):
                trigger_alert = True
            elif currentArgument in ("-o", "--get-outages"):
                get_outages = True
            elif currentArgument in ("-p", "--get-planned-interuptions"):
                get_planned_interuptions = True
        
        # if neither get_outages nor get_planned_interuptions flags are set, get_outages is the default behavior 
        if get_outages == False and get_planned_interuptions == False:
            get_outages = True
                
    except getopt.error as err:
        # output error, and return with an error code
        raise Exception(str(err))

def get_pts():
    
    #if addresses_str is empty
    if not addresses_str.strip():

        #verify path of addresses_file
        if os.path.exists(addresses_file) and os.path.isfile(addresses_file) :

            #read json contents in the file into a var
            f = open(addresses_file,'r')
            pts_str = f.read()
            f.close()
            
        else:
            #if file path is invalid raise and exception
            raise Exception('Invalid file path')
    else:
        #set pts_str to addresses_str
        pts_str = addresses_str

    #validate json data
    try:
        json.loads(pts_str)
    except ValueError as e:
        raise Exception('Json Data provided is invalid')
    
    #return json data as string
    return pts_str

if __name__ == '__main__':
    
    #variable initialization 
    outages = None
    planned_outages = None

    #process arguments and set global variables accordingly
    process_args()

    #get all the monitored pionts
    pts_str = get_pts()
    trigger_alert = True
    get_planned_interuptions = True
    #get the outages affecting the monitored points
    if get_outages:
       outages = hydroPI.get_ouatges(points_str=pts_str)

    #get any planned outages that may affect the monitored points
    if get_planned_interuptions:
        planned_outages = hydroPI.get_planned_interuptions(points_str=pts_str)

    print(planned_outages)
    #if outages list is not empty, and trigger alert option is set, trigger alert
    if trigger_alert and outages and outages != "[]":
        ''#triggers.trigger_alert_outage_print(json.loads(outages))
    
    #if planned outages list is not empty, and trigger alert option is set, trigger alert
    if trigger_alert and planned_outages and planned_outages != "[]":
        triggers.trigger_alert_planned_interuptions_print(json.loads(planned_outages))