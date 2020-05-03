from dw.utils import fp

@fp.multi
def export(dst_form, src_form, option):
    return dst_form, src_form

@fp.mmethod(export, ('tfrecord', 'old_snet'))
def export(dst_form, src_form, option):
    print('this')
    print(dst_form, src_form, option)
