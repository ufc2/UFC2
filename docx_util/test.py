from docx_merge import merge_docx

def readfile(path):
    f = open(path, 'rb')
    data = f.read()
    f.close()
    return data

result = merge_docx(readfile('base.docx'), readfile('left.docx'), readfile('right.docx'))
f = open('test.docx', 'wb')
f.write(result)
f.close()
