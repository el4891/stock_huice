import concurrent
import getopt
import inspect
import itertools
import os
import sys
import traceback
from abc import abstractmethod
from concurrent.futures.process import ProcessPoolExecutor
from datetime import datetime

import pandas as pd

print_process = False


class canshu_lei:
    def __init__(self, filename):
        self.filename = filename
        self.zhisun_rates = None
        self.max_gushu = 0
        self.zoneCalDays = 200
        self.parts = 3
        self.lines = None
        self.macd_canshu = None


class jiaoyiCelue:
    def __init__(self, pdData, canshu, result):
        self.dangqianZongjine = 0.0
        self.duotouChicang = 0
        self.kongtouChicang = 0
        self.caozuoCishu = 0
        self.jinqiXingao = 0.0
        self.jinqiXindi = 0.0
        self.data = pdData
        self.currentPrice = 0.0
        self.currentIndex = 0
        self.result = None
        self.canshu = canshu
        self.result = result
        self.yinglicishu = 0
        self.kuisuncishu = 0

    def getResult(self):
        return self.result

    @abstractmethod
    def duotouCaozuo(self):
        pass

    @abstractmethod
    def kongtouCaozuo(self):
        pass

    def caozuo(self):
        df = self.data
        tiaoguoRiqi = 256
        for i in range(df['日期'].size):
            if i > tiaoguoRiqi:
                self.currentPrice = df.loc[i, 'shoupan']
                self.currentIndex = i

                self.duotouCaozuo()
                self.kongtouCaozuo()

        self.result.loc[0, 'totalMoney'] = (self.dangqianZongjine - df.loc[df['日期'].size - 1, 'shoupan']
                                            * self.duotouChicang
                                            + df.loc[df['日期'].size - 1, 'shoupan'] * self.kongtouChicang)
        kaishiRiqi = datetime.strptime(str(df.loc[tiaoguoRiqi, '日期']), '%Y%m%d')
        jiesuRiqi = datetime.strptime(str(df.loc[df.index[-1], '日期']), '%Y%m%d')

        self.result.loc[0, 'processDay'] = (jiesuRiqi - kaishiRiqi).days
        self.result.loc[0, '操作次数'] = self.caozuoCishu
        self.result.loc[0, '当前gujia'] = df.loc[df['日期'].size - 1, 'shoupan']
        self.result.loc[0, 'sell_win'] = 0 - self.dangqianZongjine
        self.result.loc[0, 'day_win'] = self.result.loc[0, 'sell_win'] / self.result.loc[0, 'processDay']
        self.result.loc[0, 'day_win_rate'] = (self.result.loc[0, 'day_win']
                                              / df.loc[df['日期'].size - 1, f'{self.canshu.lines[4]}_junxian'])
        # if self.result.loc[0, 'day_win_rate'] > 0.00001:
        print(f'yingli --- {format(self.result.loc[0, "day_win"], ".6f")} --  '
              f'{format(self.result.loc[0, "day_win_rate"], ".6f")}  ---  {vars(self.canshu)}]\n --'
              f' yinglicishu {self.yinglicishu} -- kuisuncishu {self.kuisuncishu}, '
              f'yinglilv {self.yinglicishu / (self.yinglicishu+self.kuisuncishu)}')


class wuxianpuCelue(jiaoyiCelue):
    def __init__(self, pdData, canshu, result):
        super().__init__(pdData, canshu, result)
        self.duotouZhisun = canshu.zhisun_rates[0]
        self.kongtouZhisun = canshu.zhisun_rates[0]

    def duotouCaozuo(self):

        if (self.duotouChicang <= 0
                and self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[0]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[1]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[2]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[3]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[4]}_junxian']
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[0]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[1]}_junxian']):
            self.dangqianZongjine = self.dangqianZongjine + self.currentPrice
            self.duotouChicang = self.duotouChicang + 1
            self.caozuoCishu = self.caozuoCishu + 1

            self.jinqiXingao = 0
            # print(f'{self.data.loc[self.currentIndex, '日期']}--price {self.currentPrice}--'
            #      f'total jine {self.dangqianZongjine}--gushu{self.duotouChicang}--duo buy')

        if (self.duotouChicang > 0
                and self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[1]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[2]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[3]}_junxian']
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[1]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[2]}_junxian']):
            self.duotouZhisun = self.canshu.zhisun_rates[1]

        if (self.duotouChicang > 0
                and self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[2]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[3]}_junxian']
                < self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[4]}_junxian']
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[2]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[3]}_junxian']):
            self.duotouZhisun = self.canshu.zhisun_rates[2]

        if (self.duotouChicang > 0
                and self.jinqiXingao > 0.001
                and self.currentPrice < self.jinqiXingao * (1 - self.duotouZhisun)):
            self.dangqianZongjine = self.dangqianZongjine - self.currentPrice
            self.duotouChicang = self.duotouChicang - 1
            self.caozuoCishu = self.caozuoCishu + 1
            # print(f'{self.data.loc[self.currentIndex, '日期']}---- price {self.currentPrice}--'
            #      f'total jine {self.dangqianZongjine}--gushu{self.duotouChicang}--'
            #      f'zhisunlv--{self.duotouZhisun}--gaodian {self.jinqiXingao}--duo ping')
            self.jinqiXingao = self.currentPrice
            self.duotouZhisun = self.canshu.zhisun_rates[0]
        elif self.currentPrice > self.jinqiXingao:
            self.jinqiXingao = self.currentPrice

    def kongtouCaozuo(self):
        if (self.kongtouChicang <= 0
                and self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[0]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[1]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[2]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[3]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[4]}_junxian']
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[0]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[1]}_junxian']):
            self.dangqianZongjine = self.dangqianZongjine - self.currentPrice
            self.kongtouChicang = self.kongtouChicang + 1
            self.caozuoCishu = self.caozuoCishu + 1

            self.jinqiXindi = self.currentPrice
            # print(f'{self.data.loc[self.currentIndex, '日期']}--price {self.currentPrice}--'
            #      f'total jine {self.dangqianZongjine}--gushu{self.kongtouChicang}--kong sell')

        if (self.kongtouChicang > 0
                and self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[1]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[2]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[3]}_junxian']
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[1]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[2]}_junxian']):
            self.kongtouZhisun = self.canshu.zhisun_rates[1]

        if (self.kongtouChicang > 0
                and self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[2]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[3]}_junxian']
                > self.data.loc[self.currentIndex - 1, f'{self.canshu.lines[4]}_junxian']
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[2]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[3]}_junxian']):
            self.kongtouZhisun = self.canshu.zhisun_rates[2]

        if (self.kongtouChicang > 0
                and self.jinqiXindi > 0.001
                and self.currentPrice > self.jinqiXindi * (1 + self.kongtouZhisun)):
            self.dangqianZongjine = self.dangqianZongjine + self.currentPrice
            self.kongtouChicang = self.kongtouChicang - 1
            self.caozuoCishu = self.caozuoCishu + 1
            # print(f'{self.data.loc[self.currentIndex, '日期']}----price {self.currentPrice}--'
            #      f'total jine {self.dangqianZongjine}--gushu{self.kongtouChicang}--'
            #      f'--zhisunlv {self.kongtouZhisun}--didian{self.jinqiXindi}--kong ping')
            self.jinqiXindi = self.currentPrice
            self.kongtouZhisun = self.canshu.zhisun_rates[0]
        elif self.currentPrice < self.jinqiXindi:
            self.jinqiXindi = self.currentPrice


class suijimanbuYouceJiaoyiCelue(jiaoyiCelue):
    def __init__(self, pdData, canshu, result):
        super().__init__(pdData, canshu, result)
        self.duotouZhisun = canshu.zhisun_rates[0]
        self.kongtouZhisun = canshu.zhisun_rates[0]
        self.lengjingqi = 0
        self.lengjingqiYuzhi = canshu.zoneCalDays
        self.duotouMairuJia = 0.0
        self.kongtouMairuJia = 100000000.0

    def duotouCaozuo(self):

        self.lengjingqi = self.lengjingqi + 1

        if (self.duotouChicang <= 0 and self.kongtouChicang <= 0 and self.lengjingqi > self.lengjingqiYuzhi
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[0]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[1]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[2]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[3]}_junxian']
                > self.data.loc[self.currentIndex, f'{self.canshu.lines[4]}_junxian']):
            self.dangqianZongjine = self.dangqianZongjine + self.currentPrice
            self.duotouChicang = self.duotouChicang + 1
            self.caozuoCishu = self.caozuoCishu + 1
            self.duotouMairuJia = self.currentPrice

            self.jinqiXingao = 0
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}--'
                      f'price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.duotouChicang}--duo buy')

        if (self.duotouChicang > 0
                and self.jinqiXingao > 0.001
                and self.currentPrice < self.data.loc[self.currentIndex, f'{self.canshu.lines[3]}_junxian']):
            # and self.currentPrice < self.jinqiXingao * (1 - self.duotouZhisun)):
            self.dangqianZongjine = self.dangqianZongjine - self.currentPrice
            self.duotouChicang = self.duotouChicang - 1
            self.caozuoCishu = self.caozuoCishu + 1
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}-- '
                      f'price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.duotouChicang}--'
                      f'zhisunlv--{self.duotouZhisun}--gaodian {self.jinqiXingao}--duo ping')
            self.jinqiXingao = self.currentPrice
            self.duotouZhisun = self.canshu.zhisun_rates[0]
            self.lengjingqi = 0
            if self.duotouMairuJia < self.currentPrice:
                self.yinglicishu = self.yinglicishu + 1
            else:
                self.kuisuncishu = self.kuisuncishu + 1
        elif self.currentPrice > self.jinqiXingao:
            self.jinqiXingao = self.currentPrice

    def kongtouCaozuo(self):
        if (self.kongtouChicang <= 0 and self.duotouChicang <= 0 and self.lengjingqi > self.lengjingqiYuzhi
                and self.data.loc[self.currentIndex, f'{self.canshu.lines[0]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[1]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[2]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[3]}_junxian']
                < self.data.loc[self.currentIndex, f'{self.canshu.lines[4]}_junxian']):
            self.dangqianZongjine = self.dangqianZongjine - self.currentPrice
            self.kongtouChicang = self.kongtouChicang + 1
            self.caozuoCishu = self.caozuoCishu + 1
            self.kongtouMairuJia = self.currentPrice

            self.jinqiXindi = self.currentPrice
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}--'
                      f'price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.kongtouChicang}--kong sell')

        if (self.kongtouChicang > 0
                and self.jinqiXindi > 0.001
                and self.currentPrice > self.jinqiXindi * (1 + self.kongtouZhisun)):
            self.dangqianZongjine = self.dangqianZongjine + self.currentPrice
            self.kongtouChicang = self.kongtouChicang - 1
            self.caozuoCishu = self.caozuoCishu + 1
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}'
                      f'--price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.kongtouChicang}--'
                      f'--zhisunlv {self.kongtouZhisun}--didian{self.jinqiXindi}--kong ping')
            self.jinqiXindi = self.currentPrice
            self.kongtouZhisun = self.canshu.zhisun_rates[0]
            self.lengjingqi = 0
            if self.kongtouMairuJia > self.currentPrice:
                self.yinglicishu = self.yinglicishu + 1
            else:
                self.kuisuncishu = self.kuisuncishu + 1
        elif self.currentPrice < self.jinqiXindi:
            self.jinqiXindi = self.currentPrice


class macdJiaochaCelue(jiaoyiCelue):
    def __init__(self, pdData, canshu, result):
        super().__init__(pdData, canshu, result)
        self.duotouZhisun = canshu.zhisun_rates[0]
        self.kongtouZhisun = canshu.zhisun_rates[0]
        self.lengjingqi = 0
        self.lengjingqiYuzhi = canshu.zoneCalDays
        self.duotouMairuJia = 0.0
        self.kongtouMairuJia = 100000000.0


    def duotouCaozuo(self):

        self.lengjingqi = self.lengjingqi + 1

        # dif_name = f'{macdZuhe[0]}_dif'
        # dea_name = f'{macdZuhe[0]}_dea'
        # macd_name = f'{macdZuhe[0]}_macd'
        if (self.duotouChicang <= 0 and self.kongtouChicang <= 0 and self.lengjingqi > self.lengjingqiYuzhi
                and self.data.loc[self.currentIndex, f'{self.canshu.macd_canshu[0]}_dif']
                > self.data.loc[self.currentIndex, f'{self.canshu.macd_canshu[0]}_dea']):
            self.dangqianZongjine = self.dangqianZongjine + self.currentPrice
            self.duotouChicang = self.duotouChicang + 1
            self.caozuoCishu = self.caozuoCishu + 1
            self.duotouMairuJia = self.currentPrice

            self.jinqiXingao = 0
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}--'
                      f'price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.duotouChicang}--duo buy')

        if (self.duotouChicang > 0
                and self.jinqiXingao > 0.001
               # and self.currentPrice < self.data.loc[self.currentIndex, f'{self.canshu.lines[3]}_junxian']):
                and self.currentPrice < self.jinqiXingao * (1 - self.duotouZhisun)):
            self.dangqianZongjine = self.dangqianZongjine - self.currentPrice
            self.duotouChicang = self.duotouChicang - 1
            self.caozuoCishu = self.caozuoCishu + 1
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}-- '
                      f'price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.duotouChicang}--'
                      f'zhisunlv--{self.duotouZhisun}--gaodian {self.jinqiXingao}--duo ping')
            self.jinqiXingao = self.currentPrice

            self.duotouZhisun = self.canshu.zhisun_rates[0]
            self.lengjingqi = 0
            if self.duotouMairuJia < self.currentPrice:
                self.yinglicishu = self.yinglicishu + 1
            else:
                self.kuisuncishu = self.kuisuncishu + 1
        elif self.currentPrice > self.jinqiXingao:
            self.jinqiXingao = self.currentPrice

    def kongtouCaozuo(self):
        if (self.kongtouChicang <= 0 and self.duotouChicang <= 0 and self.lengjingqi > self.lengjingqiYuzhi
                and self.data.loc[self.currentIndex, f'{self.canshu.macd_canshu[0]}_dif']
                < self.data.loc[self.currentIndex, f'{self.canshu.macd_canshu[0]}_dea']):
            self.dangqianZongjine = self.dangqianZongjine - self.currentPrice
            self.kongtouChicang = self.kongtouChicang + 1
            self.caozuoCishu = self.caozuoCishu + 1
            self.kongtouMairuJia = self.currentPrice

            self.jinqiXindi = self.currentPrice
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}--'
                      f'price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.kongtouChicang}--kong sell')

        if (self.kongtouChicang > 0
                and self.jinqiXindi > 0.001
                and self.currentPrice > self.jinqiXindi * (1 + self.kongtouZhisun)):
            self.dangqianZongjine = self.dangqianZongjine + self.currentPrice
            self.kongtouChicang = self.kongtouChicang - 1
            self.caozuoCishu = self.caozuoCishu + 1
            if print_process:
                print(f'{self.data.loc[self.currentIndex, "日期"]}--{self.data.loc[self.currentIndex, "shijian"]}'
                      f'--price {self.currentPrice}--'
                      f'total jine {self.dangqianZongjine}--gushu{self.kongtouChicang}--'
                      f'--zhisunlv {self.kongtouZhisun}--didian{self.jinqiXindi}--kong ping')
            self.jinqiXindi = self.currentPrice
            self.kongtouZhisun = self.canshu.zhisun_rates[0]
            self.lengjingqi = 0
            if self.kongtouMairuJia > self.currentPrice:
                self.yinglicishu = self.yinglicishu + 1
            else:
                self.kuisuncishu = self.kuisuncishu + 1
        elif self.currentPrice < self.jinqiXindi:
            self.jinqiXindi = self.currentPrice


class process_lei:
    def __init__(self, canshu):
        self.canshu = canshu
        self.currentP = 0.0
        self.heightP = 0.0
        self.heightPIndex = 0
        self.secondHeightP = 0.0
        self.secondHiPIndex = 0
        self.lowP = 1000000.0
        self.lowPIndex = 0
        self.secondLowP = 1000000.0
        self.secondLowPIndex = 0
        self.topCtrlLine = 0
        self.buttonCtrlLine = 0

    def process(self, currentP, currentIndex):
        self.currentP = currentP

        lowOrHiChange = False

        if currentP > self.heightP:
            self.heightP = currentP
            self.heightPIndex = currentIndex
            lowOrHiChange = True
            self.secondHeightP = 0
            self.secondHiPIndex = 0
        else:
            if (currentIndex - self.heightPIndex) > (self.canshu.zoneCalDays / 3):
                if currentP > self.secondHeightP:
                    self.secondHeightP = currentP
                    self.secondHiPIndex = currentIndex

            if (currentIndex - self.heightPIndex) > self.canshu.zoneCalDays:
                self.heightP = self.secondHeightP
                self.heightPIndex = self.secondHiPIndex
                lowOrHiChange = True
                self.secondHeightP = 0
                self.secondHiPIndex = 0

        if currentP < self.lowP:
            self.lowP = currentP
            self.lowPIndex = currentIndex
            lowOrHiChange = True
            self.secondLowP = 1000000
            self.secondLowPIndex = 0
        else:
            if (currentIndex - self.lowPIndex) > (self.canshu.zoneCalDays / 3):
                if currentP < self.secondLowP:
                    self.secondLowP = currentP
                    self.secondLowPIndex = currentIndex

            if (currentIndex - self.lowPIndex) > self.canshu.zoneCalDays:
                self.lowP = self.secondLowP
                self.lowPIndex = self.secondLowPIndex
                lowOrHiChange - True
                self.secondLowP = 1000000
                self.secondLowPIndex = 0

        if lowOrHiChange:
            qujian = (self.heightP - self.lowP) / self.canshu.parts
            self.topCtrlLine = self.heightP - qujian
            self.buttonCtrlLine = self.lowP + qujian

    def isTopZone(self):
        if self.currentP > self.topCtrlLine:
            return True
        else:
            return False

    def isButtomZone(self):
        if self.currentP < self.buttonCtrlLine:
            return True
        else:
            return False


def print_line_number():
    frame = inspect.currentframe()
    lineno = frame.f_back.f_lineno
    print(f'Line{lineno}')


def get_average_line(df: pd.DataFrame, days):
    print(f'{get_average_line.__name__} -- {days}')
    col_name = f'{days}_junxian'
    data_return = pd.DataFrame(columns=['日期', col_name])

    data_return['shijian'] = df['shijian']
    data_return['日期'] = df['日期']
    data_return[col_name] = df['shoupan'].rolling(window=days).mean()

    # 保留小数点后几位
    data_return = data_return.round(decimals=5)
    return data_return


def jieguoChuli(result, canshu):
    result.loc[0, 'junxian1'] = canshu.lines[0]
    result.loc[0, 'junxian2'] = canshu.lines[1]
    result.loc[0, 'junxian3'] = canshu.lines[2]
    result.loc[0, 'junxian4'] = canshu.lines[3]
    result.loc[0, 'junxian5'] = canshu.lines[4]
    result.loc[0, 'gu票'] = canshu.filename
    result.loc[0, '止sun1'] = canshu.zhisun_rates[0]
    result.loc[0, '止sun2'] = canshu.zhisun_rates[1]
    result.loc[0, '止sun3'] = canshu.zhisun_rates[2]
    result.loc[0, '最大gu数'] = canshu.max_gushu
    result.loc[0, 'zone_day'] = canshu.zoneCalDays
    result.loc[0, 'parts'] = canshu.parts

    if result.loc[0, 'day_win_rate'] > 0.00001:
        # 获取当前时间并格式化为字符串
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
        # 生成的目录路径
        directory = "out"
        if not os.path.exists(directory):
            os.makedirs(directory)
        result = result.sort_values('sell_win', ascending=False)
        result.to_csv(f'{directory}/{canshu.filename}_{now}_result.csv')


def process_data(canshu, df_in):
    df = df_in

    celueJishu = 0

    # try:
    #     result = pd.DataFrame(
    #         columns=['gu票', 'totalMoney', '总gu数', '平均gujia', '当前gujia', 'sell_win', 'processDay', 'day_win'])
    #
    #     celue = suijimanbuYouceJiaoyiCelue(df, canshu, result)
    #     celue.caozuo()
    #     result = celue.getResult()
    #     result.loc[0, f'celueMing{celueJishu}'] = celue.__class__.__name__
    #     celueJishu = celueJishu + 1
    #     jieguoChuli(result, canshu)
    # except Exception as e:
    #     traceback.print_exc()

    try:
        result = pd.DataFrame(
            columns=['gu票', 'totalMoney', '总gu数', '平均gujia', '当前gujia', 'sell_win', 'processDay', 'day_win'])

        celue = macdJiaochaCelue(df, canshu, result)
        celue.caozuo()
        result = celue.getResult()
        result.loc[0, f'celueMing{celueJishu}'] = celue.__class__.__name__
        celueJishu = celueJishu + 1
        jieguoChuli(result, canshu)
    except Exception as e:
        traceback.print_exc()

    return 1


def junxianJisuan(dfSrc, list):
    df = dfSrc
    with ProcessPoolExecutor(max_workers=16) as executor:
        results = {executor.submit(get_average_line, dfSrc, i): i for i in list}

        for future in concurrent.futures.as_completed(results):
            linei = future.result()
            df = pd.merge(df, linei, on=['日期', 'shijian'])

    return df


def EMA(data, window):
    return data.ewm(span=window, min_periods=window, adjust=False).mean()


def macdJisuanProcess(df: pd.DataFrame, macdZuhe):
    print(f'{macdJisuanProcess.__name__} -- {macdZuhe}')
    dif_name = f'{macdZuhe[0]}_dif'
    dea_name = f'{macdZuhe[0]}_dea'
    macd_name = f'{macdZuhe[0]}_macd'
    data_return = pd.DataFrame(columns=['日期'])

    data_return['shijian'] = df['shijian']
    data_return['日期'] = df['日期']

    data_return[dif_name] = EMA(df['shoupan'], macdZuhe[1]) - EMA(df['shoupan'], macdZuhe[2])
    data_return[dea_name] = EMA(data_return[dif_name], macdZuhe[3])
    data_return[macd_name] = (data_return[dif_name] - data_return[dea_name]) * 2

    # 保留小数点后几位
    data_return = data_return.round(decimals=5)
    return data_return


def macdJisuan(dfSrc, macd_list):
    df = dfSrc
    with ProcessPoolExecutor(max_workers=16) as executor:
        results = {executor.submit(macdJisuanProcess, dfSrc, i): i for i in macd_list}

        for future in concurrent.futures.as_completed(results):
            linei = future.result()
            df = pd.merge(df, linei, on=['日期', 'shijian'])

    return df


def line_product(filename):
    line1_list = [2]
    line2_list = [16]
    line3_list = [64]
    line4_list = [128]
    line5_list = [256]
    zhisun_rate_list1 = [0.015]
    zhisun_rate_list2 = [0.3]
    zhisun_rate_list3 = [0.4]
    max_gushu_list = [8]
    zoneCalDaysList = [1]
    parts_list = [3]
    macd_list = [[0, 12, 26, 9], [1, 24, 52, 18]]

    print(filename)
    filepath = 'line/' + filename
    fileOutPath = f'data_out/data_{filename}.csv'
    df = None
    if os.path.exists(fileOutPath):
        df = pd.read_csv(fileOutPath, header=0, low_memory=False)
    else:
        df = pd.read_csv(filepath, header=1, low_memory=False)
        df = df.iloc[:-1]
        df['kaipan'] = df['开盘'].astype(float)
        df = df.drop(columns='开盘')
        df['zuigao'] = df['最高'].astype(float)
        df = df.drop(columns='最高')
        df['zuidi'] = df['最低'].astype(float)
        df = df.drop(columns='最低')
        df['shoupan'] = df['收盘'].astype(float)
        df = df.drop(columns='收盘')
        if '时间' in df.columns.tolist():
            df['shijian'] = df['时间']
            df = df.drop(columns='时间')
        else:
            df['shijian'] = 8

        junxianlist = [2, 8, 16, 32, 64, 128, 256, 512, 1024]
        df = junxianJisuan(df, junxianlist)
        df = macdJisuan(df, macd_list)
        if not os.path.exists('data_out'):
            os.makedirs('data_out')
        df.to_csv(fileOutPath)

    with ProcessPoolExecutor(max_workers=16) as executor:
        for i in itertools.product(line1_list, line2_list, line3_list, line4_list, line5_list,
                                   zhisun_rate_list1, zhisun_rate_list2, zhisun_rate_list3,
                                   max_gushu_list, zoneCalDaysList, parts_list, macd_list):
            if i[0] < i[1] < i[2] < i[3] < i[4] and i[5] < i[6] < i[7]:
                try:
                    canshu = canshu_lei(filename)
                    canshu.lines = [i[0], i[1], i[2], i[3], i[4]]
                    canshu.zhisun_rates = [i[5], i[6], i[7]]
                    canshu.max_gushu = i[8]
                    canshu.zoneCalDays = i[9]
                    canshu.parts = i[10]
                    canshu.macd_canshu = i[11]
                    print(vars(canshu))
                    executor.submit(process_data, canshu, df)
                except Exception as e:
                    traceback.print_exc()


def process():
    filelist = None
    for i, j, k in os.walk('line'):
        filelist = k
        print(filelist)

    # pool = multiprocessing.Pool(multiprocessing.cpu_count())
    #
    for filename in filelist:
        #     pool.apply_async(func=line_product, args=(filename,))
        line_product(filename)
    # pool.close()
    # pool.join()


if __name__ == '__main__':
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'a:', 'print_process=')
    except:
        print('error')

    for opt, arg in opts:
        if opt in ['-a', 'print_process']:
            if int(arg) == 0:
                print_process = False
            else:
                print_process = True

    process()
