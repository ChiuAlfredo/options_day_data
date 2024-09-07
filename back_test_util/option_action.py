
class Option:
    def __init__(self,inintial_cash):
        self.inintial_cash = inintial_cash
        self.profit_total = 0
        self.position_list = []
        self.record_list = []
    
    def buy_call(self,timestamp,strike_price,price,quantity,fee):
        profit = -price*quantity-fee
        self.profit_total += profit
        self.record_list.append({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        self.position_list.append({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price,'是否該賣':False})   
        print({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        return price,self.profit_total
    def sell_call(self,timestamp,strike_price,price,quantity,fee):
        profit = price*quantity-fee
        self.profit_total += profit
        self.record_list.append({'時間':timestamp,'BorS':'S','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        self.position_list.pop()
        print({'時間':timestamp,'BorS':'S','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        return price,self.profit_total
    
    def buy_put(self,timestamp,strike_price,price,quantity,fee):
        profit = -price*quantity-fee
        self.profit_total += profit
        self.record_list.append({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        self.position_list.append({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price,'是否該賣':False})   
        print({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        return price,self.profit_total
    
    def sell_put(self,timestamp,strike_price,price,quantity,fee):
        profit = price*quantity-fee
        self.profit_total += profit
        self.record_list.append({'時間':timestamp,'BorS':'S','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        self.position_list.pop()
        print({'時間':timestamp,'BorS':'S','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total})
        return price
        
    