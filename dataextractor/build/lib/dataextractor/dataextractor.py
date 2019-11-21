import pandas as pd
import numpy as np
import re
import csv


class extractVars:

    def __init__(self, rawFile, resultFile, convDpoint, delim=';', header=None,
                 names=['dpoint', 'timestamp', 'cookie_id', 'source',
                        'medium', 'keyword', 'campaign']):
                        assert isinstance(rawFile, str)
                        assert isinstance(resultFile, str)
                        assert isinstance(convDpoint, int)
                        assert isinstance(delim, str)
                        assert isinstance(names, list)

                        self.rawFile = rawFile
                        self.resultFile = resultFile
                        self.convDpoint = convDpoint
                        self.delim = delim
                        self.header = header
                        self.names = names

    def parseDatastream(self):
        with open(self.rawFile, mode='r') as rf:
            rFile = csv.reader(rf)

            for line in rFile:
                vector = re.sub("_", ";", str(line))
                vector = re.split(";", str(vector))

                with open(self.resultFile, mode='a') as nf:
                    nFile = csv.writer(nf, delimiter=';')
                    nFile.writerow(vector[1:8])

    def extractConversionTime(self):
        streamTime = pd.read_csv(self.resultFile, sep=self.delim,
                                 header=self.header, names=self.names)

        streamTime = streamTime[['cookie_id', 'dpoint', 'timestamp',
                                 'source', 'medium', 'keyword', 'campaign']]

        firstVisit = streamTime.groupby('cookie_id', as_index=False)
        firstVisit = firstVisit.agg({'timestamp': 'min'})

        firstConversion = streamTime[streamTime['dpoint'] == self.convDpoint]
        firstConversion = firstConversion.groupby('cookie_id', as_index=False)
        firstConversion = firstConversion.agg({'timestamp': 'min'})

        streamConvTime = pd.merge(firstVisit, firstConversion, on='cookie_id')
        streamConvTime.columns = ['cookie_id', 'fVisit', 'convTime']
        streamConvTime['convDuration'] = streamConvTime['convTime'] - streamConvTime['fVisit']

        return streamConvTime

    def extractVisits(self):
        streamVisits = pd.read_csv(self.resultFile, sep=self.delim,
                                   header=self.header, names=self.names)

        streamVisits = streamVisits[['cookie_id', 'dpoint', 'timestamp',
                                     'source', 'medium', 'keyword', 'campaign']]

        streamVisits = streamVisits.groupby(['cookie_id', 'dpoint'],
                                            as_index=False)['timestamp'].count()

        streamVisits.columns = ['cookie_id', 'dpoint', 'no_visits']

        return streamVisits

    def extractTime(self):
        subpageTime = pd.read_csv(self.resultFile, sep=self.delim,
                                  header=self.header, names=self.names)

        subpageTime = subpageTime[['cookie_id', 'dpoint', 'timestamp']]
        timeContainer = pd.DataFrame()
        print(str(len(np.unique(subpageTime.cookie_id))) + ' number of users \n')
        n = 0
        for user in np.unique(subpageTime['cookie_id']):
            n += 1
            if n % 1000 == 0:
                print(str(n) + ' users read.')
            ustream = subpageTime[subpageTime['cookie_id'] == user]
            ustream = ustream.sort_values('timestamp', ascending=True)
            ustream = ustream.reset_index(drop=True)
            ustream['nextTimestamp'] = None
            for time in range(len(ustream)-1):
                ustream.loc[time, 'nextTimestamp'] = ustream.loc[time+1, 'timestamp']
            timeContainer = timeContainer.append(ustream)

        subpageStream = pd.merge(subpageTime,
                                 timeContainer[['cookie_id', 'timestamp',
                                                'nextTimestamp']],
                                                 on=['cookie_id', 'timestamp'],
                                                 how='left')
        subpageStream['timeOnSite'] = subpageStream['nextTimestamp']/1000 - subpageStream['timestamp']/1000
        print('\nFinished! ')

        return subpageStream


ext = extractVars('res.csv', 'res3.csv', convDpoint=125690265)
ext.parseDatastream()
conv_time = ext.extractConversionTime()
conv_time.head()
visits = ext.extractVisits()
visits.head()
timeOnSubpage = ext.extractTime()
