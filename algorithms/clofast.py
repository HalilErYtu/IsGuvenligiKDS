from pymining import itemmining

class Clofast:

    def __init__(self, transactions, min_sup=2,):
        self.min_sup = min_sup
        self.seq = transactions
        self.out = {}

    def setminsup(self, minsup=2, default_minsup = False):
        if default_minsup:
            minsup = 0.015 # denenmiş değerlere göre belirlendi. 
        if (minsup < 1):
            self.min_sup = int(len(self.seq) * minsup)
        else:
            self.min_sup = minsup
        print(self)

    def get_result(self):
        return (str(list(e)) + ' : ' + str(self.out[e]) for e in self.out)

    def frequent_item_set_mining(self):
        relim_input = itemmining.get_relim_input(self.seq)
        report = itemmining.relim(relim_input, self.min_sup)
        print(report)
        self.out = report


def prepare_clofast_data(raw_data: str):
    s = raw_data.replace('\t', ' ')
    s = s.replace('\n', ' ')
    s = s.replace('\t', '')
    s = s.replace('\r', '')
    s = s.replace('\ufeff', '')
    s = s.strip()
    s = s.replace(' ', '')
    s = ''.join(s.split())
    s = s.replace('-1', ' ')
    d1 = s.split('-2')
    # d1.pop()
    d2 = []
    for i in d1:
        d2.append(i.split(' '))
    return d2
