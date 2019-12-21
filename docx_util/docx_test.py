from docx_merge import  merge_docx

fbase = open('base.docx', 'rb')
base = fbase.read()
fbase.close()

fleft = open('left.docx', 'rb')
left = fleft.read()
fleft.close()

fright = open('right.docx', 'rb')
right = fright.read()
fright.close()

result = merge_docx(base, left, right)

fresult = open('result.docx', 'wb')
fresult.write(result)
fresult.close()
