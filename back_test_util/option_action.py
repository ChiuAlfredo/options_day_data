
class Option:
    def __init__(self):
        self.profit_total = {'option':0,'future':0}
        
        # 部位 平倉就會刪除
        self.position_list = {'option':[],'future':{'B':{},'S':{}}}
        self.record_list = {'option':[],'future':[]}
        
    def _add_opsition(self,product,condition):
        if condition['數量']>0:
            if product =='option':
            
                self.position_list[product].append(condition)
                
            
            if product == 'future':
                if self.position_list[product][condition['BorS']]=={}:
                    self.position_list[product][condition['BorS']]= condition
                else:
                    self.position_list[product][condition['BorS']]['價格'] = (self.position_list[product][condition['BorS']]['價格']*self.position_list[product][condition['BorS']]['數量']+condition['價格']*condition['數量'])/(self.position_list[product][condition['BorS']]['數量']+condition['數量'])
                    self.position_list[product][condition['BorS']]['數量'] += condition['數量']
            return True
        if condition['數量']<=0:
            return False
                
                
        
    def _remove_position(self,product,condition):
        if product == 'option':
            for index, item in enumerate(self.position_list[product]):
                if all(k in ['數量', '價格', '現價'] or item.get(k) == v for k, v in condition.items() if k in ['BorS', '履約價', 'CorP']):
                    self.position_list[product][index]['數量'] -= condition['數量']
                    if self.position_list[product][index]['數量'] <= 0:
                        del self.position_list[product][index]
                    return True
            return False
        elif product == 'future':
            if self.position_list[product][condition['BorS']] != {}:
                if self.position_list[product][condition['BorS']]['數量']>condition['數量']:
                    self.position_list[product][condition['BorS']]['數量'] -= condition['數量']
                else:
                    self.position_list[product][condition['BorS']] = {}
                return True
            else:
                return False
    
    def option_trade(self,timestamp,BorS,CorP,strike_price,price,quantity,fee,position_action,record_dict={}):
        
        codition = {'BorS':BorS,'履約價':strike_price,'CorP':CorP,'價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price}
        print(codition)
        if BorS == 'B':
            profit = -price*quantity-fee
            self.profit_total['option'] += profit
            if position_action == 'open':
                action_success = self._add_opsition(product = 'option',condition = codition)
            elif position_action == 'close':
                codition['BorS'] = 'S'
                action_success = self._remove_position('option',codition)
        elif BorS == 'S':
            profit = price*quantity-fee
            self.profit_total['option'] += profit
            if position_action == 'open':
                action_success = self._add_opsition(product = 'option',condition = codition)
            elif position_action == 'close':
                 
                codition['BorS'] = 'B'
                action_success = self._remove_position('option',codition)
        print(action_success)
        if action_success ==True:
            record = {'時間':timestamp,'BorS':BorS,'倉位狀況':position_action,'履約價':strike_price,'CorP':CorP,'價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']}
            record.update(record_dict)
            # print(record)
            self.record_list['option'].append(record)
            return price,self.profit_total['option']
        elif action_success == False:
            return False
        
    def future_trade(self,timestamp,BorS,price,quantity,fee,position_action,record_dict={}):
        codition = {'BorS':BorS,'價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price}
        print(codition)
        if BorS == 'B':
            profit = -price*quantity-fee
            self.profit_total['future'] += profit
            if position_action == 'open':
                action_success = self._add_opsition(product = 'future',condition = codition)
            elif position_action == 'close':
                codition['BorS'] = 'S'
                action_success = self._remove_position('future',codition)
        elif BorS == 'S':
            profit = price*quantity-fee
            self.profit_total['future'] += profit
            if position_action == 'open':
                action_success = self._add_opsition(product = 'future',condition = codition)
                
            elif position_action == 'close':
                codition['BorS'] = 'B'
                action_success = self._remove_position('future',codition)
        print(action_success)
        if action_success ==True:
            record = {'時間':timestamp,'BorS':BorS,'倉位狀況':position_action,'價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['future']}
            record.update(record_dict)
            self.record_list['future'].append(record)
            
            return True
        elif action_success == False:
            return False
    
            
        
    
    # def buy_call(self,timestamp,strike_price,price,quantity,fee,position_action):
    #     profit = -price*quantity-fee
    #     self.profit_total['option'] += profit
    #     self.record_list['option'].append({'時間':timestamp,'BorS':'B','倉位狀況':position_action,'履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     if position_action == 'open':
    #         self._add_opsition(product = 'option',condition = {'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
            
    #     elif position_action == 'close':
    #         self._remove_position('option',{'BorS':'S','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     print({'時間':timestamp,'BorS':'B','倉位狀況':position_action,'履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     return price,self.profit_total['option']
    
    # def sell_call(self,timestamp,strike_price,price,quantity,fee,position_action):
    #     profit = price*quantity-fee
    #     self.profit_total['option'] += profit
    #     self.record_list['option'].append({'時間':timestamp,'BorS':'S','倉位狀況':position_action,'履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     if position_action == 'open':
    #         self._add_opsition('option',{'BorS':'S','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
           
    #     elif position_action == 'close':
    #         self._remove_position('option',{'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
       
    #     print({'時間':timestamp,'BorS':'S','倉位狀況':position_action,'履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     return price,self.profit_total['option']
    
    # def buy_put(self,timestamp,strike_price,price,quantity,fee,position_action):
    #     profit = -price*quantity-fee
    #     self.profit_total['option'] += profit
    #     self.record_list.append({'時間':timestamp,'BorS':'B','倉位狀況':position_action,'履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     if position_action == 'open':
    #         self._add_opsition('option',{'BorS':'B','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     elif position_action == 'close':
    #         self._remove_position('option',{'BorS':'S','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
        
    #     print({'時間':timestamp,'BorS':'B','倉位狀況':position_action,'履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     return price,self.profit_total['option']
    
    # def sell_put(self,timestamp,strike_price,price,quantity,fee,position_action):
    #     profit = price*quantity-fee
    #     self.profit_total['option'] += profit
    #     self.record_list['option'].append({'時間':timestamp,'BorS':'S','倉位狀況':position_action,'履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     if position_action == 'open':
    #         self._add_opsition('option',{'BorS':'S','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     elif position_action == 'close':
    #         self._remove_position('option',{'BorS':'B','履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     print({'時間':timestamp,'BorS':'S','倉位狀況':position_action,'履約價':strike_price,'CorP':'P','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['option']})
    #     return price,self.profit_total['option']
        
    # def buy_future(self,timestamp,price,quantity,fee,position_action):
    #     profit = -price*quantity-fee
    #     self.profit_total['future'] += profit
    #     self.record_list['future'].append({'時間':timestamp,'BorS':'B','倉位狀況':position_action,'價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['future']})
    #     if position_action == 'open':
    #         self._add_opsition('future',{'BorS':'B','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     elif position_action == 'close':
    #         self._remove_position('future',{'BorS':'S','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     print({'時間':timestamp,'BorS':'B','倉位狀況':position_action,'價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['future']})
    #     return price,self.profit_total['future']
    
    # def sell_future(self,timestamp,price,quantity,fee,position_action):
    #     profit = price*quantity-fee
    #     self.profit_total['future'] += profit
    #     self.record_list['future'].append({'時間':timestamp,'BorS':'S','倉位狀況':position_action,'價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['future']})
    #     if position_action == 'open':
    #         self._add_opsition('future',{'BorS':'S','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    #     elif position_action == 'close':
    #         self._remove_position('future',{'BorS':'B','價格':price,'數量':quantity,'現價':price,'最高價':price,'最低價':price})
    
    #     print({'時間':timestamp,'BorS':'S','倉位狀況':position_action,'價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':self.profit_total['future']})
    #     return price,self.profit_total['future']