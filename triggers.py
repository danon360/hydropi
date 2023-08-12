#write youre trigger functions here 

def trigger_alert_outage_print(outages: list):
    for outage in outages:
        alais = outage['alais']
        address = outage['address']
        start = outage['start']
        end = outage['end']
        number_affected = outage['num_affected']
        status = outage['status']
        cause = outage['cause']
        municipality_code = outage['municipality_code']

        print("Outage currently occuring at %s \nStart Time: %s \nEnd Time %s \nCurrent Status: %s \nCause: %s \nNumber of affected people: %s"\
               % (alais,start,end,status,cause,number_affected))


def trigger_alert_planned_interuptions_print(interuptions: list):
    for interuption in interuptions:
        alais = interuption['alais']
        address = interuption['address']
        scheduled_start = interuption['scheduled_start']
        scheduled_end = interuption['scheduled_end']
        actual_start = interuption['actual_start']
        actual_end = interuption['actual_end']
        postponed_start = interuption['postponed_start']
        postponed_end = interuption['postponed_end']
        number_affected = interuption['num_affected']
        status = interuption['status']
        cause = interuption['cause']
        municipality_code = interuption['municipality_code']

        print("A planned service interuption is scheduled to occure at %s \
              \nScheduled Start Time: %s \nScheduled End Time %s \nPostponed Start Time: %s \nPostponed End Time: %s \
              \nCurrent Status: %s \nCause: %s \nNumber of affected people: %s"\
               % (alais,scheduled_start,scheduled_end,postponed_start,postponed_end,status,cause,number_affected))
