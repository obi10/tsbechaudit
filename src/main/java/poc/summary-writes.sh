rm -f /home/opc/Documents/tsbechaudit/log/read_thread.csv

for i in /home/opc/Documents/tsbechaudit/log/read*.log
do 
   sed -i '$ d' $i
   cat $i >> /home/opc/Documents/tsbechaudit/log/read_thread.csv
done

rm -f /home/opc/Documents/tsbechaudit/log/write_thread.csv

for i in /home/opc/Documents/tsbechaudit/log/write*.log
do
   sed -i '$ d' $i
   cat $i >> /home/opc/Documents/tsbechaudit/log/write_thread.csv
done

python3 summary-writes.py
