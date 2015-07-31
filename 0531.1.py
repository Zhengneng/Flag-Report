import pandas as pd, inspect, csv, mmap ,time, smtplib, pytz, datetime

from collections import defaultdict
from time import gmtime, strftime
from email.mime.text import MIMEText
from pytz import timezone
from datetime import datetime

#len(df['day'])/20

def data_input_and_calculation():
    # data input and clean
    input_time_start = datetime.now(eastern) #record the start eastern time, for log purpose
    global logList
    for i in range(0,len(df['day'])/5): # first one fifth
        try:
            if i % 5000 == 0: # update the log every 5000 rows reading
                time_now = datetime.now(eastern) # record time
                now = time_now.strftime("%Y-%m-%d_%I:%M:%S")
                logList.append(['Read rows * 5000',i,str(now)])
                with open('/tmp/0604/log_1.csv','wb') as f:
                    writer = csv.writer(f)
                    writer.writerows(logList)
                f.close()
            # to see if the combination key (deal type, deal, campaign, domain) is in the dict or not
            if (df.iloc[i,9],df.iloc[i,8],df.iloc[i,1],df.iloc[i,2]) not in domainVCPM.keys():
                cumCost = float(df.iloc[i,4])
                cumImp_App = float(df.iloc[i,3])
                cumInView = float(df.iloc[i,5])
                cumMeas = float(df.iloc[i,6])
                if cumInView == 0:
                    cumViewability = 0.0000001
                elif cumMeas == 0:
                    cumViewability = 100
                else:
                    cumViewability = cumInView/cumMeas           
                if cumCost == 0:
                    cumCPM = 0.0000001
                elif cumImp_App == 0:
                    cumCPM == 10000000
                else:
                    cumCPM = cumCost/cumImp_App*1000
                cumVCPM = cumCPM/cumViewability
                    
            else:               
                cumCost += float(df.iloc[i,4])
                cumImp_App += float(df.iloc[i,3])
                cumInView += float(df.iloc[i,5])
                cumMeas += float(df.iloc[i,6])
                if cumInView == 0:
                    cumViewability = 0.0000001
                elif cumMeas == 0:
                    cumViewability = 100
                else:
                    cumViewability = cumInView/cumMeas           
                if cumCost == 0:
                    cumCPM = 0.0000001
                elif cumImp_App == 0:
                    cumCPM == 10000000
                else:
                    cumCPM = cumCost/cumImp_App*1000
                cumVCPM = cumCPM/cumViewability
            domainVCPM[(df.iloc[i,9],df.iloc[i,8],df.iloc[i,1],df.iloc[i,2])]= [cumVCPM,cumCPM,cumViewability,cumCost,cumImp_App,cumMeas,cumInView]
            if cumVCPM > 2.52:
                indexList.append([df.iloc[i,0],df.iloc[i,9],df.iloc[i,8],df.iloc[i,1],df.iloc[i,2],"Bad"])
            else:
                indexList.append([df.iloc[i,0],df.iloc[i,9],df.iloc[i,8],df.iloc[i,1],df.iloc[i,2],"Good"])

        except Exception as e:
            ny_time = datetime.now(eastern)
            now = ny_time.strftime("%Y-%m-%d_%I:%M:%S")
            errorList.append(str(e)+' '+str(now))
            logList.append(str(e)+' '+str()now))
            continue

    input_time_end = datetime.now(eastern) #record the end eastern time, for log purpose
    now = input_time_end.strftime("%Y-%m-%d_%I:%M:%S")
    runningTime = input_time_end-input_time_start # calculate the running time

    # send log email
    if len(errorList) > 0: 
        MText = str(runningTime)+'        '.join(errorList)
        subject = 'Display SSH Cal Error! '
            
    else:
        MText = 'Successfully!  Running time: '+str(runningTime)
        subject = 'Display SSH Cal Success! '
    email_sender(MText,subject)


def statistics():
    # To see how many consecutive days bad or good
    tag_time_start = datetime.now(eastern) #record the start eastern time, for log purpose
    global count
    global logList
    i = -1
    for row in indexList:
        i += 1
        try:
            if i % 5000 == 0:
                time_now = datetime.now(eastern)
                now = time_now.strftime("%Y-%m-%d_%I:%M:%S")
                logList.append(['statistics rows * 5000', i ,str(now)])
                with open('/tmp/0604/log_1.csv','wb') as f:
                    writer = csv.writer(f)
                    writer.writerows(logList)
                f.close()
            if row[5] == "Bad":
                if (row[1],row[2],row[3],row[4]) not in attention.keys():
                    attention[(row[1],row[2],row[3],row[4])] = 1
                else:
                    gap = int(float(row[0])) - int(float(record_attention[(row[1],row[2],row[3],row[4])]))
                    if gap == 86400:
                        attention[(row[1],row[2],row[3],row[4])] += 1
                        if attention[(row[1],row[2],row[3],row[4])] == 7:
                            result_attention[(row[1],row[2],row[3],row[4])] = 7
                        else:
                            result_attention[(row[1],row[2],row[3],row[4])] = attention[(row[1],row[2],row[3],row[4])]
                    else:
                        attention[(row[1],row[2],row[3],row[4])] = 1
                record_attention[(row[1],row[2],row[3],row[4])] = row[0]
            else:
                if (row[1],row[2],row[3],row[4]) not in well.keys():
                    well[(row[1],row[2],row[3],row[4])] = 1
                else:
                    gap = int(float(row[0])) - int(float(record_well[(row[1],row[2],row[3],row[4])]))
                    if gap == 86400:
                        well[(row[1],row[2],row[3],row[4])] += 1
                        if well[(row[1],row[2],row[3],row[4])] == 7:
                            result_well[(row[1],row[2],row[3],row[4])] = 7
                        else:
                            result_well[(row[1],row[2],row[3],row[4])] = well[(row[1],row[2],row[3],row[4])]
                    else:
                        well[(row[1],row[2],row[3],row[4])] = 1
                record_well[(row[1],row[2],row[3],row[4])] = row[0]
            count += 1
        except Exception as e:
            ny_time = datetime.now(eastern)
            now = ny_time.strftime("%Y-%m-%d_%I:%M:%S")
            errorList.append(str(e)+' '+str(now))
            logList.append(str(e)+' '+str()now))
            continue

    tag_time_end = datetime.now(eastern) #record the end eastern time, for log purpose
    now = tag_time_end.strftime("%Y-%m-%d_%I:%M:%S")
    runningTime = tag_time_end-tag_time_start

    if len(errorList) > 0:
        MText = str(runningTime)+'        '.join(errorList)
        subject = 'Display SSH statistics Error! '
            
    else:
        MText = 'Successfully!  Running time: '+str(runningTime)
        subject = 'Display SSH statistics Success! '
    email_sender(MText,subject)


def flag():
    # flag it based on the statistics
    red = 0
    orange = 0
    green = 0
    light_green = 0

    score_view = [ (v,k) for k,v in result_attention.iteritems()]
    score_view.sort(reverse=True)

    print "=============Attention Ranking==========================="

    for v,k in score_view:
        #print "%s: %d" % (k,v)
            if v == 7:
                    red += 1
            else:
                    orange += 1
    print "\n" 

    score_view = [ (v,k) for k,v in result_well.iteritems() ]
    score_view.sort(reverse=True)

    print "==============Well Ranking==============================="

    for v,k in score_view:
            #print "%s: %d" % (k,v)
            if v == 7:
                    green += 1
            else:
                    light_green += 1
    print "\n"

    #in_f.close()

    print "red: ", red
    print "orange: ", orange
    print "green: ", green
    print "light_green: ", light_green
    print "yellow: ", count - red - orange - green - light_green

    """for keys, values in domain.items():
            print(keys)
            print "{:.2%}".format(values)
    """
    #====================================================================================
    countkeyAtt = 0
    for key in result_attention.keys():
            countkeyAtt += 1
            #print key, domainVCPM[key]
    print "Attentions: ", countkeyAtt
    countkeyWel = 0
    for key in result_well.keys():
            countkeyWel += 1
            #print key, domainVCPM[key]
    print "Well: ", countkeyWel
    global logList
    time_now = datetime.now(eastern)
    now = time_now.strftime("%Y-%m-%d_%I:%M:%S")
    logList.append(['Count Done','Time: ',str(now)])
    with open('/tmp/0604/log_1.csv','wb') as f:
        writer = csv.writer(f)
        writer.writerows(logList)
    f.close()


def writing():
    # write data into CSV file
    writing_time_start = datetime.now(eastern) #record the start eastern time, for log purpose

    try:
        path = '/tmp/0604/'
        FileRed = '0531red.1.csv'
        FileGreen = '0531green.1.csv'
        CSV_writer(result_attention, path, FileRed, 'countRed')
        CSV_writer(result_well, path, FileGreen, 'countGreen')
        global logList
        time_now = datetime.now(eastern)
        now = time_now.strftime("%Y-%m-%d_%I:%M:%S")
        logList.append(['Writing Done','Time: ',str(now)])
        with open('/tmp/0604/log_1.csv','wb') as f:
            writer = csv.writer(f)
            writer.writerows(logList)
        f.close()
    except Exception as e:
        ny_time = datetime.now(eastern)
        now = ny_time.strftime("%Y-%m-%d_%I:%M:%S")
        errorList.append(str(e)+' '+str(now))
        logList.append(str(e)+' '+str()now))
        pass


    writing_time_end = datetime.now(eastern) #record the end eastern time, for log purpose
    now = writing_time_end.strftime("%Y-%m-%d_%I:%M:%S")
    runningTime = writing_time_end-writing_time_start

    if len(errorList) > 0:
            MText = str(runningTime)+'        '.join(errorList)
            subject = 'Display SSH Writing Error! '
            
    else:
            MText = 'Successfully!  Running time: '+str(runningTime)
            subject = 'Display SSH Writing Success! '
    email_sender(MText,subject)
       

def email_sender(MText,subject):
    # email credential setting
    time_now = datetime.now(eastern)
    now = time_now.strftime("%Y-%m-%d_%I:%M:%S")
    msg = MIMEText(MText, _subtype='html')
    msg['Subject'] = subject +str(filename)+"_"+str(now)
    msg['From'] = username
    msg['To'] = ";".join(to_list)
    server = smtplib.SMTP(host)
    server.starttls()
    server.login(username, password)
    server.sendmail(username, to_list, msg.as_string())
    server.quit()


def CSV_writer(result, Path, File, countType):
    # CSV writing setting
    count = 0
    wList = []
    wList.append(["Deal Type","Deal","Campaigns","Domains","vCPM","CPM","Viewability","Spend","Impressions","Measureable Imp","Viewable Imp"])
    for key, value in sorted(domainVCPM.items(), key=lambda e: e[1][0], reverse = True):
            if key in result.keys() and result[key] == 7:
                    count += 1
                    wList.append([key[0],key[1],key[2],key[3],value[0],value[1],value[2],value[3],value[4],value[5],value[6]])
                   # print key, value
    print countType + ": ", count

    with open(Path+File,'wb') as f:
            writer = csv.writer(f)
            writer.writerows(wList)
    f.close()


if __name__ == "__main__":
    to_list=["qiuzhengneng@gmail.com"]
    host="smtp.gmail.com:587"
    username='groupm.usa@gmail.com'
    password='******'
    attention = {}
    record_attention = {}
    record_well = {}
    result_attention = {}
    result_well = {}
    well = {}
    logList = []
    count = 0
    domainVCPM = defaultdict(list)
    indexList = []
    eastern = timezone('US/Eastern')
    errorList = []
    filename = inspect.getfile(inspect.currentframe())
    #len(df['day'])
    df = pd.read_csv('/tmp/0604/0531.csv')
    data_input_and_calculation()
    statistics()
    flag()
    writing()
