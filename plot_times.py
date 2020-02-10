import pandas as pd
import matplotlib.pyplot as plt
import json
def plot_times(f_names):
     for i in f_names:

          times_list = list(json.loads(open(i).read()))
          threads = [list(j.keys())[0] for j in times_list]
          threads = list(map(int,threads))
          times = [list(j.values())[0] for j in times_list]
          print(times)
          print(threads)
          #x = pd.DataFrame(list(zip(threads, times)), columns=['No. of Threads', 'Execution Time'])
          #plot = x.plot(figsize=(16, 16), x='No. of Threads', y='Execution Time')
          plt.plot(times,threads,label=i)
          plt.legend(i)
     #times_series.plot()
     plt.title('Multi-Threading Approaches')
     plt.ylabel('Execution Time in seconds')
     plt.xlabel('Number of Threads')
     plt.show()

if __name__=='__main__':
    plot_times(['times_mt_ap1.json','times_mt_ap2.json'])