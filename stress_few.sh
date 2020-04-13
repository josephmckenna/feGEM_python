#It seems I can eat 8Gb of ram when goes higher than 15?
for j in `seq 0 10`; do 
for i in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z; do 
python3 test_few_variables.py ${j}${i} &> Few_${j}_${i}.log & sleep 10; 
done
done && sleep 60 && for i in `seq 1 5000`; do kill %${i}; done
