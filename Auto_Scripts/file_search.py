valid_attachments = (".xyz")
l=[]
path=["D:/"]

for i in path:
    f = os.listdir(str(i))
    for file_name in f:
        if file_name.lower().endswith(valid_attachments):
            l.append(file_name)
    if l == []:
        print(i,"path is empty")
    else:
        print(i,"contains")

