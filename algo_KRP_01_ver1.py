from MyTT import KDJ, RSI, PSY
from pandas.io.json import json_normalize

from predefined_functions.defined_functionality import Defined_Functionality
from datetime import datetime, timedelta, time
import pandas as pd
import numpy as np
import time
import requests
np.seterr(divide='ignore', invalid='ignore')
pd.set_option('mode.chained_assignment', None)

from trading_ig import IGService


class Algo0:
    def __init__(self):
        self.df = Defined_Functionality()

        self.list_of_epics = ['IX.D.NASDAQ.IFD.IP'] #'IX.D.DOW.DAILY.IP','CC.D.CT.UNC.IP'
        self.df.set_epics_to_look_for(epic_list=self.list_of_epics)

        self.map_epic_data_minute={} #Dictionary
        for epic in self.list_of_epics:
            self.map_epic_data_minute[epic] = pd.DataFrame() #List
            #print("datatype = ", type(self.map_epic_data_minute[epic]))

        self.first_timestamp = None  ### Program is running for the first time
        print("Init")

    def telegram_bot_sendtext(bot_message):
        bot_token = ''
        bot_chatID = ''
        send_text = 'https://api.telegram.org/bot' + bot_token +'/sendMessage?chat_id=' + bot_chatID +\
            '&parse_mode=MarkdownV2&text=' + bot_message
        response = requests.get(send_text)

        #telegram_bot_sendtext("context")

        return response.json()

    def run(self):

        while(True):
            try:
                epic = self.list_of_epics[0]
                signals = self.signal_generation(epic)
                self.create_positions(epic=epic, signals_levels=signals)
                self.closing_positions(epic=epic, signals= signals)
                time.sleep(56)
                print("Run")

            except Exception as e:
                print(e, "   error in the looping for the defined_functionality")

    def closing_positions(self, epic, signals):
        position = self.df.find_open_position_by_epic(epic=epic)
        if len(position) == 0:
            #time.sleep(56)
            return

        if isinstance(position,pd.core.series.Series):
            if signals == None:
                #time.sleep(56)
                #print("position already exists", position)
                return

            if (signals["BUY"] != None and (position["direction"] == "SELL")): #We have buy signal and sell position, close sell position
                self.df.close_position(position=position)
                self.telegram_bot_sendtext("Exit")
                #time.sleep(56)

            elif (signals["SELL"] != None and (position["direction"] == "BUY")):
                self.df.close_position(position=position)
                self.telegram_bot_sendtext("Exit")
                #time.sleep(56)



    def create_positions(self, epic, signals_levels):
        print("Create")
        if signals_levels == None:
            return
        key = None
        if signals_levels["BUY"] != None:
            key = "BUY"
        elif signals_levels["SELL"] != None:
            key = "SELL"


        position = self.df.find_open_position_by_epic(epic=epic)

        if len(position) != 0:
            return position


        create_position = self.df.create_open_position(epic="IX.D.NASDAQ.IFD.IP", direction=key, size=0.5, force_open=True)
        self.telegram_bot_sendtext(bot_message = "Buy")
        return create_position


    def signal_generation(self, epic):
        global trade_status, LFL, rqm, RV, sn, LB, LFLE, LE, LFS, SB, LFSE, SE, SLR
        signals_levels = None
        # minute_10 = 60 * 10
        # minute_10 = 60
        minute_10 = 10*60 ### This is timer, sec as unit
        datetime_now = datetime.now()
        data = None
        print("SignalGen")
        #print(len(self.map_epic_data_minute[epic]))

        if (self.first_timestamp != None):  ### Check if program is running for the first time. (self.first_timestamp = None) means first time
            difference = (datetime_now - self.first_timestamp)

            if (difference.seconds > minute_10):  ### If this is not the first datapoint that is being loaded, then append new datapoint to dataframe
                response = self.df.get_historical_data_via_num_points(epic=epic, resolution="10Min", num_points=1)
                self.first_timestamp = datetime_now
                response = response.drop(columns=[('ask', 'Open'),
                                                                                                ('ask', 'High'),
                                                                                                ('ask', 'Low'),
                                                                                                ('ask', 'Close'),
                                                                                                ('last', 'Open'),
                                                                                                ('last', 'High'),
                                                                                                ('last', 'Low'),
                                                                                                ('last', 'Close'), ])
                response.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

                #Check if the datas are all the same
                '''if ((response['Open'][0] == self.map_epic_data_minute[epic]['Open'][-1]) or (response['Close'][0] == self.map_epic_data_minute[epic]['Close'][-1])\
                    or (response['High'][0] == self.map_epic_data_minute[epic]['High'][-1]) or (response['Low'][0] == self.map_epic_data_minute[epic]['Low'][-1])\
                    or (response['Volume'][0] == self.map_epic_data_minute[epic]['Volume'][-1])):
                        print("Market closed or data repeat")'''

                if (response.index[0].value == self.map_epic_data_minute[epic].index[-1].value):
                        print("Market closed or data repeat")

                else:
                    self.map_epic_data_minute[epic] = pd.concat([self.map_epic_data_minute[epic], response], axis=0)
                    print(response)
                    #print(self.map_epic_data_minute[epic]['Open'][-1])
                    print(self.first_timestamp)


        else:  ### Program is running for the first time, get historical data for 50 data points
            response = self.df.get_historical_data_via_num_points(epic=epic, resolution="10Min", num_points=50)
            self.first_timestamp = datetime_now
            print(self.first_timestamp)
            response = response.drop(columns=[('ask', 'Open'),
                                                                                            ('ask', 'High'),
                                                                                            ('ask', 'Low'),
                                                                                            ('ask', 'Close'),
                                                                                            ('last', 'Open'),
                                                                                            ('last', 'High'),
                                                                                            ('last', 'Low'),
                                                                                            ('last', 'Close'), ])
            response.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            self.map_epic_data_minute[epic] = pd.concat([self.map_epic_data_minute[epic], response], axis=0)
            print(len(self.map_epic_data_minute[epic]))
            print(self.map_epic_data_minute[epic])
            print(response)

            ##-----Setting variables for entry/exist-----##
            trade_status = 0
            RV = 0  # Record value for tracing the highest and lowest
            rqm = 99  # lower the stricter
            sn = 1  # Sensitivity for entry and exit, higher the stricter

            LFL = 0  # Look for long
            LB = 0  # Long begin
            LFLE = 0  # Look for long exit
            LE = 0  # Long exit

            LFS = 0  # Look for short
            SB = 0  # Short begin
            LFSE = 0  # Look for short exit
            SE = 0  # Short

            SLR = 70  # Stoploss requirement, higher the stricter

        if len(self.map_epic_data_minute[epic]) > 49:  ### If the length of the dataframe is greater than 49, remove the oldest datapoint
            #self.map_epic_data_minute[epic].pop(0)

            sell_level = None
            buy_level = None

            #----------The section below add KRP into the dataframe----------#
            #self.map_epic_data_minute[epic].to_csv('KRPData4.csv')
            #print("create csv")
            self.map_epic_data_minute[epic]['KDJ'] = KDJ(self.map_epic_data_minute[epic]['Close'], self.map_epic_data_minute[epic]['High'], self.map_epic_data_minute[epic]['Low'], N=9, M1=3, M2=3)
            self.map_epic_data_minute[epic]['RSI'] = RSI(self.map_epic_data_minute[epic]['Close'], N=24)
            self.map_epic_data_minute[epic]['PSY'] = PSY(self.map_epic_data_minute[epic]['Close'], N=12, M=6)
            self.map_epic_data_minute[epic]['SUM'] = self.map_epic_data_minute[epic]['KDJ'] + self.map_epic_data_minute[epic]['RSI'] + self.map_epic_data_minute[epic]['PSY']
            self.map_epic_data_minute[epic]['SIG'] = float()

            # ----------The section below caculate signal and put it in the dataframe----------#

            for i in range(17, len(self.map_epic_data_minute[epic])):
                #print("chk1", i)
                sig = 0.0
                # KDJ
                if self.map_epic_data_minute[epic]['KDJ'][i] <= 20:
                    sig = sig - 1
                elif self.map_epic_data_minute[epic]['KDJ'][i] <= 40:
                    sig = sig - 0.5
                elif self.map_epic_data_minute[epic]['KDJ'][i] <= 60:
                    sig = sig
                elif self.map_epic_data_minute[epic]['KDJ'][i] <= 80:
                    sig = sig + 0.5
                else:
                    sig = sig + 1

                # RSI
                if self.map_epic_data_minute[epic]['RSI'][i] <= 20:
                    sig = sig - 1
                elif self.map_epic_data_minute[epic]['RSI'][i] <= 40:
                    sig = sig - 0.5
                elif self.map_epic_data_minute[epic]['RSI'][i] <= 60:
                    sig = sig
                elif self.map_epic_data_minute[epic]['RSI'][i] <= 80:
                    sig = sig + 0.5
                else:
                    sig = sig + 1

                # PSY
                if self.map_epic_data_minute[epic]['PSY'][i] <= 20:
                    sig = sig - 1
                elif self.map_epic_data_minute[epic]['PSY'][i] <= 40:
                    sig = sig - 0.5
                elif self.map_epic_data_minute[epic]['PSY'][i] <= 60:
                    sig = sig
                elif self.map_epic_data_minute[epic]['PSY'][i] <= 80:
                    sig = sig + 0.5
                else:
                    sig = sig + 1

                self.map_epic_data_minute[epic]['SIG'][i] = sig
                #self.map_epic_data_minute[epic].replace({self.map_epic_data_minute[epic]['SIG'][i]}, sig, inplace=True)
                #self.map_epic_data_minute[epic].loc['DateTime'[i], 'SIG'] = sig
                #print("chk2")

                if i == len(self.map_epic_data_minute[epic]):
                    break
                #else:
                    #break



            #----------The section below trades---------#
            self.map_epic_data_minute[epic].to_csv('KRPData.csv')
            #print("chk3")
            ##-----Entry long-----##
            # Entry Long
            # Requirement
            if (self.map_epic_data_minute[epic]['SIG'][-1] <= -2 or LFL == 1) and trade_status == 0 \
                    and self.map_epic_data_minute[epic]['KDJ'][-1] <= rqm and self.map_epic_data_minute[epic]['RSI'][
                -1] <= rqm and self.map_epic_data_minute[epic]['PSY'][-1] <= rqm:
                LFL = 1
                if self.map_epic_data_minute[epic]['SUM'][-1] < RV:
                    RV = self.map_epic_data_minute[epic]['SUM'][-1]
                elif self.map_epic_data_minute[epic]['SUM'][-1] - RV > sn:
                    LFL = 0
                    LB = 1
            # Execute long
            if LB == 1:
                buy_level = 1
                trade_status = 1
                LB = 0
                RV = 0
                print("Enter Long")

            ##-----Exist long----##
            # Exit Long
            # Requirement
            #print("chk4")
            if (self.map_epic_data_minute[epic]['SIG'][-1] >= 0 or LFLE == 1) and trade_status == 1:
                LFLE = 1
                if self.map_epic_data_minute[epic]['SUM'][-1] > RV:
                    RV = self.map_epic_data_minute[epic]['SUM'][-1]
                elif RV - self.map_epic_data_minute[epic]['SUM'][-1] > sn:
                    LFLE = 0
                    LE = 1
            # Execute exit long
            if LE == 1:
                #self.closing_positions(epic, "BUY")
                position = self.df.find_open_position_by_epic(epic=epic)
                self.df.close_position(position=position[0])
                trade_status = 0
                LE = 0
                RV = 0
                OTP = 0
                print("Exit Long normally")

            ##-----Entry short-----##
            # Short entry
            # Requirement
            if (self.map_epic_data_minute[epic]['SIG'][-1] >= 2 or LFS == 1) and trade_status == 0 \
                    and self.map_epic_data_minute[epic]['KDJ'][-1] >= (100 - rqm) and \
                    self.map_epic_data_minute[epic]['RSI'][-1] >= (100 - rqm) and \
                    self.map_epic_data_minute[epic]['PSY'][
                        -1] >= (100 - rqm):
                LFS = 1
                if self.map_epic_data_minute[epic]['SUM'][-1] > RV:  # 向下反轉，找最大值
                    RV = self.map_epic_data_minute[epic]['SUM'][-1]
                elif RV - self.map_epic_data_minute[epic]['SUM'][-1] > sn:
                    LFS = 0
                    SB = 1
            SB = 1
            # Execute short
            if SB == 1:
                sell_level = 1
                trade_status = -1
                SB = 0
                RV = 0
                print("Entry short")

            ##-----Exit short-----##
            # Exit Short
            # Requirement
            #print("chk4")
            if (self.map_epic_data_minute[epic]['SIG'][-1] <= 0 or LFSE == 1) and trade_status == -1:
                LFSE = 1
                if self.map_epic_data_minute[epic]['SUM'][-1] < RV:
                    RV = self.map_epic_data_minute[epic]['SUM'][-1]
                elif self.map_epic_data_minute[epic]['SUM'][-1] - RV > sn:
                    LFSE = 0
                    SE = 1
            # Execute short
            if SE == 1:
                position = self.df.find_open_position_by_epic(epic=epic)
                self.df.close_position(position=position[0])
                #self.closing_positions(epic, "SELL")
                trade_status = 0
                SE = 0
                RV = 0
                OTP = 0
                print("Exit short normally")

            ######-----Stoploss-----#######
            ##-----Stoploss exit long-----##
            # Stoploss Exit Long
            # Requirement
            if (self.map_epic_data_minute[epic]['KDJ'][-1] >= SLR or self.map_epic_data_minute[epic]['RSI'][
                -1] >= SLR or self.map_epic_data_minute[epic]['PSY'][-1] >= SLR) \
                    and trade_status == 1:
                # if (sig >=0 or LFLE ==1) and trade_status == 1:
                LFLE = 0
                LE = 1
            # Execute exit long
            if LE == 1:
                #self.closing_positions(epic, "BUY")
                position = self.df.find_open_position_by_epic(epic=epic)
                self.df.close_position(position=position[0])
                trade_status = 0
                LE = 0
                RV = 0
                OTP = 0
                print("Exit long by stoploss")

            ##-----Stoploss exit short-----##
            # Stoploss Exit Short
            # Requirement
            if (self.map_epic_data_minute[epic]['KDJ'][-1] <= (100 - SLR) or self.map_epic_data_minute[epic]['RSI'][
                -1] <= (100 - SLR) or self.map_epic_data_minute[epic]['PSY'][-1] <= (
                        100 - SLR)) \
                    and trade_status == -1:
                LFSE = 0
                SE = 1
            # Execute short
            if SE == 1:
                position = self.df.find_open_position_by_epic(epic=epic)
                print(position)
                self.df.close_position(position=position[0])
                #self.closing_positions(epic, "SELL")
                trade_status = 0
                SE = 0
                RV = 0
                OTP = 0
                print("Exit short by stoploss")

            if (sell_level == None) and (buy_level == None):
                return None

            signals_levels = {
                "SELL": sell_level,
                "BUY": buy_level
            }

        return signals_levels
