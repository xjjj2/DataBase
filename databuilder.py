file=open("some_data_file.txt",'w')
for i in range(0,100):
    for j in range(0,100):
        file.write(str(i)+"0"+str(j))
        if (j!=99):file.write(",")
    file.write('\r\n')
file.close()
        