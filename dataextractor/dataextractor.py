import pandas as pd
import numpy as np
import re
import csv


class extractVars:
    """
    Class for extracting new variables out of datastream file

    This class is created to extract 'conversion duration' - users time since
    first visit till first conversion, 'visits frequency' - number of visits on
    particular sub-/page and 'session time' - how much time did pass from
    one visit till the next visit or going to website subpage

    ...

    Attributes
    ----------
    resultFile : str
        Any valid string path in csv-like format

    convDpoint : int
        Any datapoint number in integer-like format that indicates user
        conversion

    deli : str
        Delimiter in the upload file, defaults to ';'

    names : list
        List of column names to use, defaluts to
        '['dpoint', 'timestamp', 'cookie_id', 'source', 'medium',
        'keyword', 'campaign']'

    Methods
    -------

    extractConversionTime(self)
        Takes input as csv-like file (resultFile) and outputs dataframe with
        additional column (Conversion duration) that shows how long did it
        take for user to make the first conversion.

    extractVisits(self)
        Takes input as csv-like file (resultFile) and outputs dataframe with
        additional column (Amount of visits) that shows how many times has
        user visited particular datapoint.

    extractSessionTime(self)
        Takes input as csv-like file (resultFile) and outputs dataframe with
        additional column (Session time) that shows how long did someone spent
        on particular datapoint before he entered website again or went to
        another subpage.

    extractUtm(self, column)
        Takes input as csv-like file (resultFile) and outputs pivoted DataFrame
        with columns presenting utm indicators and how many times has someone
        entered the site using possible sources.

    extractAll(self, column)
        Launching all the above methods, extracts all possible variables
        and merge results into one dataframe.

    """

    def __init__(self, rawFile, resultFile, convDpoint, delim=';',
                 names=['dpoint', 'timestamp', 'cookie_id', 'source',
                        'medium', 'keyword', 'campaign'], index_col=False):
        assert isinstance(rawFile, str)
        assert isinstance(resultFile, str)
        assert isinstance(convDpoint, int)
        assert isinstance(delim, str)
        assert isinstance(names, list)

        self.rawFile = rawFile
        self.resultFile = resultFile
        self.convDpoint = convDpoint
        self.delim = delim
        self.names = names

    def parseDatastream(self, pattern, repl):
        assert isinstance(pattern, str)
        assert isinstance(repl, str)

        with open(self.rawFile, mode='r') as rf:
            rFile = csv.reader(rf)

            for line in rFile:
                vector = re.sub(pattern, repl, str(line))
                vector = re.split(repl, str(vector))

                with open(self.resultFile, mode='a') as nf:
                    nFile = csv.writer(nf, delimiter=repl)
                    nFile.writerow(vector[1:8])

    def extractConversionTime(self):
        ''' Extracts conversion time from input file and returns dataframe

        Description
        -----------
        Results are always placed in RAM, thus serving very large files may
        occure not possible.
        '''

        streamTime = pd.read_csv(self.resultFile, sep=self.delim,
                                 names=self.names, index_col=False)
        streamTime = streamTime[['cookie_id', 'dpoint', 'timestamp']]

        # creates dataframe with column thet indicates time of first visit
        # on website
        firstVisit = streamTime.groupby('cookie_id', as_index=False)\
                               .agg({'timestamp': 'min', })

        # selects users who landed on conversion datapoint (thank you page)
        firstConversion = streamTime[streamTime['dpoint'] == self.convDpoint]

        # creates dataframe with column that indicates first time conversion
        # on website
        firstConversion = firstConversion.groupby('cookie_id', as_index=False)\
                                         .agg({'timestamp': 'min'})

        # creates dataframe with column that indicates last nextTimestamp
        lastVisit = streamTime.groupby('cookie_id', as_index=False)\
                              .agg({'timestamp': 'max'})

        streamConvTime = pd.merge(firstVisit, firstConversion, on='cookie_id')
        streamConvTime = pd.merge(streamConvTime, lastVisit, on='cookie_id')

        # renaming columns
        streamConvTime.columns = ['cookie_id', 'fVisit', 'convTime', 'lVisit']
        # calculating how long did it take for a user to make a conversion
        streamConvTime['convDuration'] = streamConvTime['convTime'] - \
            streamConvTime['fVisit']

        return streamConvTime

    def extractVisits(self):
        ''' Extracts amount of visits for every user on every served datapoint

        Description
        -----------
        Results are always placed in RAM, thus serving very large files may
        occure not possible.
        '''

        streamVisits = pd.read_csv(self.resultFile, sep=self.delim,
                                   names=self.names, index_col=False)
        streamVisits = streamVisits[['cookie_id', 'dpoint', 'timestamp']]

        # calculating number of vistis
        streamVisits = streamVisits.groupby(['cookie_id', 'dpoint'],
                                            as_index=False)['timestamp'].count()
        # renaming columns
        streamVisits.columns = ['cookie_id', 'dpoint', 'no_visits']

        # pivoting columns
        streamVisits = streamVisits.pivot('cookie_id', 'dpoint', 'no_visits')
        streamVisits = pd.DataFrame(streamVisits.to_records())

        return streamVisits

    def extractSessionTime(self):
        ''' Extracts session time on every datapoint that includes input file

        Description
        -----------
        Results are always placed in RAM, thus serving very large files may
        occure not possible.
        '''

        subpageTime = pd.read_csv(self.resultFile, sep=self.delim,
                                  names=self.names, index_col=False)
        subpageTime = subpageTime[['cookie_id', 'dpoint', 'timestamp']]
        # printing number of unique users in input file
        print(str(len(np.unique(subpageTime.cookie_id))) + ' number of users')
        print('\n')
        # sorting by time to perform shift function
        subpageTime = subpageTime.sort_values('timestamp', ascending=True)
        # adding a column and shifting up timestamp column in order to be able
        # to count how long did user spend on particular subpage or how much
        # time has passed since he visite website last time
        subpageTime['nextTimestamp'] = subpageTime.groupby('cookie_id')[
                                        'timestamp'].shift(periods=-1, axis=0)

        # dividing by 1000 to gain results in seconds
        # (expected miliseconds format)
        subpageTime['timeOnSite'] = subpageTime['nextTimestamp'] / \
            1000 - subpageTime['timestamp'] / 1000

        subpageTime = subpageTime.groupby(['cookie_id', 'dpoint'],
                                          as_index=False).agg({'timeOnSite': 'mean'})

        subpageTime = subpageTime.pivot('cookie_id', 'dpoint', 'timeOnSite')
        subpageTime = pd.DataFrame(subpageTime.to_records())

        print('\nFinished! ')

        return subpageTime

    def extractUtm(self, column):
        ''' Extracts utm's on every datapoint that includes input file

        Description
        -----------
        Results are always placed in RAM, thus serving very large files may
        occure not possible.

        Parameters
        ----------
        column : str
            Name of column that should be extracted and transformed.
            Available: source, medium, source/medium, keyword, campaign

        '''
        assert isinstance(column, str)

        streamUtm = pd.read_csv(self.resultFile, sep=self.delim,
                                names=self.names, index_col=False)
        streamUtm = streamUtm[['cookie_id', 'timestamp', 'source', 'medium',
                               'keyword', 'campaign']]

        streamUtm['source/medium'] = streamUtm['source'].map(str) + '/' + \
            streamUtm['medium'].map(str)
        streamUtm = streamUtm.groupby(['cookie_id', column], as_index=False)[
            'timestamp'].count()

        # renaming columns
        streamUtm.columns = ['cookie_id', 'a', 'no_visits']

        streamUtm = streamUtm.pivot('cookie_id', 'a', 'no_visits')
        streamUtm = pd.DataFrame(streamUtm.to_records())

        return streamUtm

    def extractAll(self, column='source'):
        ''' Extracts all available extra variables mined by other methods

        Description
        -----------
        Results are always placed in RAM, thus serving very large files may
        occure not possible.

        Parameters
        ----------
        column : str
            Name of column that should be extracted and transformed.
            Available: source, medium, source/medium, keyword, campaign

        '''
        assert isinstance(column, str)

        df1 = self.extractConversionTime()
        df2 = self.extractVisits()
        df3 = self.extractSessionTime()
        df4 = self.extractUtm(column)
        all = df1.merge(df2, on='cookie_id').merge(df3, on='cookie_id')\
                 .merge(df4, on='cookie_id')

        return all
