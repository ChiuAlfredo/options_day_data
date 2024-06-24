import numpy as np
from scipy.stats import norm
class BS_formula:
    def __init__(self, s0, k, r, sigma, T):     
        self.s0 = s0 # 標的物價格
        self.k = k # 履約價格
        self.r = r # 無風險利率
        self.sigma = sigma # 歷史波動度
        self.T = T # 剩餘到期時間
        self.d1 = (np.log(s0/k)+(r+sigma**2/2)*T) / (sigma * np.sqrt(T))
        self.d2 = ((np.log(s0/k)+(r+sigma**2/2)*T) / (sigma * np.sqrt(T))) - sigma*np.sqrt(T)
        
    def BS_price(self): # 計算理論價格
        c = self.s0*norm.cdf(self.d1) - self.k*np.exp(-self.r*self.T)*norm.cdf(self.d2)
        p = self.k*np.exp(-self.r*self.T)*norm.cdf(-self.d2) - self.s0*norm.cdf(-self.d1)
        return c,p
        
    def BS_delta(self): # 計算 delta
        return norm.cdf(self.d1), norm.cdf(self.d1)-1
    
    def BS_gamma(self): # 計算 gamma
        return norm.pdf(self.d1)/(self.s0*self.sigma*np.sqrt(self.T)), norm.pdf(self.d1)/(self.s0*self.sigma*np.sqrt(self.T))
    
    def BS_vega(self): # 計算 vega
        return self.s0*np.sqrt(self.T)*norm.pdf(self.d1), self.s0*np.sqrt(self.T)*norm.pdf(self.d1)
    
    def BS_theta(self): # 計算 theta 
        c_theta = -self.s0*norm.pdf(self.d1)*self.sigma / (2*np.sqrt(self.T)) - self.r*self.k*np.exp(-self.r*self.T)*norm.cdf(self.d2)
        p_theta = -self.s0*norm.pdf(self.d1)*self.sigma / (2*np.sqrt(self.T)) + self.r*self.k*np.exp(-self.r*self.T)*norm.cdf(-self.d2)
        return c_theta, p_theta
    
    def BS_rho(self): # 計算 rho  
        return self.k*self.T*np.exp(-self.r*self.T)*norm.cdf(self.d2), -self.k*self.T*np.exp(-self.r*self.T)*norm.cdf(-self.d2)
