rm /home/opc/IdeaProjects/tsbenchmysql-poc-meli/log/read_thread.csv

for i in /home/opc/IdeaProjects/tsbenchmysql-poc-meli/log/read*.log
do 
   sed -i '$ d' $i
   cat $i >> /home/opc/IdeaProjects/tsbenchmysql-poc-meli/log/read_thread.csv
done

rm /home/opc/IdeaProjects/tsbenchmysql-poc-meli/log/write_thread.csv

for i in /home/opc/IdeaProjects/tsbenchmysql-poc-meli/log/write*.log
do
   sed -i '$ d' $i
   cat $i >> /home/opc/IdeaProjects/tsbenchmysql-poc-meli/log/write_thread.csv
done

python3 summary.py
