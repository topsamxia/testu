count = 0

s=5
for i in range(s):
    for k in range(s):
        for j in range(s):
            for l in range(s):
                for m in range(s):
                    number=(i+1)*10000+(k+1)*1000+(j+1)*100+(l+1)*10+(m+1)
                    if ('33' in str(number)) or ('44' in str(number)):
                        print(number)
                    else:
                        count += 1

print(count)


