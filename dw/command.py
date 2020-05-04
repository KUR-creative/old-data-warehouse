from collections import namedtuple

import numpy as np
import funcy as F
from pypika import Table, Query

from dw.utils import fp
from dw import db

Dataset = namedtuple(
    'Dataset',
    'name split train valid test',
    defaults=[None, None, None]
)

def map_pixels(img, cond_color, true_color, false_color=None):
    '''
    Make mask from img pixels that have cond_color, and replace with true_color
    '''
    h,w,c = img.shape
    cond_color  = [[cond_color]]
    true_color  = [[true_color]]
    false_color = (np.zeros_like(true_color) 
                   if false_color is None else false_color)
    dst_c = false_color.shape[-1]
    t_pixel = np.ones_like(cond_color)
    f_pixel = np.zeros_like(cond_color)
    go = lambda x,*fs: F.rcompose(*fs)(x)
    return go(
        cond_color,
        # make mask
        lambda c: np.repeat(c, h*w, axis=0), 
        lambda m: m.reshape([h,w,c]),
        # make where array 
        lambda m: np.all(img == m, axis=-1),
        lambda w: np.expand_dims(w, axis=-1),
        lambda w: np.repeat(w, c, axis = -1),
        # make t/f map
        lambda w: np.where(w, t_pixel, f_pixel),
        lambda m: np.expand_dims(m[:,:,0], axis=-1),
        lambda m: np.repeat(m, dst_c, axis=-1),
        # make return value
        lambda r: r * true_color,
        lambda r: r.astype(np.uint8),
    )

@F.autocurry
def map_colors(src_dst_colormap, img): 
    """
    Map img color space w.r.t. src_dst_colormap.
    
    Map colors of `img` w.r.t. `src_dst_colormap`.
    src_dst_colormap: {src1:dst1, src2:dst2, ...}
    """
    # Preconditions
    dic = src_dst_colormap
    assert ((type(dic) is bidict) or (type(dic) is dict) and 
            type(img) is np.ndarray), \
        'type(src_dst_colormap) = {}, type(img) = {}'.format(
            type(dic), type(img))
    assert unique_color_set(img) <= set(map( tuple, dic.keys() )), \
            (' img = {} > {} = dic \n It means some pixels in img' 
            +' cannot be mapped with this rgb<->1hot dict').format( 
                unique_color_set(img), set(map( tuple, dic.keys() )))

    h,w,_ = img.shape
    some_dst_color = next(iter(src_dst_colormap.values()))
    c_dst = len(some_dst_color)

    ret_img = np.zeros((h,w,c_dst), dtype=img.dtype)
    for c, (src_bgr, dst_color) in enumerate(src_dst_colormap.items()):
        mapped = map_pixels(img, src_bgr, dst_color)
        ret_img += mapped

    return ret_img

def _bytes_feature(value):
    '''Returns a bytes_list from a string / byte.'''
    if isinstance(value, type(tf.constant(0))):
        value = value.numpy() # BytesList won't unpack a string from an EagerTensor.
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))
def _float_feature(value):
    '''Returns a float_list from a float / double.'''
    return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))
def _int64_feature(value):
    '''Returns an int64_list from a bool / enum / int / uint.'''
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

def generate(train_path_pairs, valid_path_pairs, test_path_pairs,
        src_dst_colormap, out_path, look_and_feel_check=False):
    '''
    Generate dataset using image, mask path pairs 
    from `train_path_pairs`, `valid_path_pairs`, `test_path_pairs`.
    Masks are mapped by src_dst_colormap. Finally, dataset is saved to `out_path`.

    `src_dst_colormap` is encoded and saved in src_rgbs and dst_1hots.
    imag, mask pairs are saved in [train-pairs, valid-pairs, test-pairs] sequence.

    output dataset is 
    [
        {num_train: TR,
         num_valid: VA,
         num_test:  TE,
         num_class:  N}
        {src_rgbs:  [ 0x??, 0x??, .. ]
         dst_1hots: [ 1, 2, 4, 8.. ]}
        {h:, w:, c:, mc:, img:, mask:}
        {h:, w:, c:, mc:, img:, mask:}
        ...
    ]

    If look_and_feel_check == True, load and display 
    image and masks in generated dataset.

    Image, mask paths are must be paired, and exists.
    '''
    # Preconditions
    img_paths, mask_paths = fp.unzip(
        train_path_pairs + valid_path_pairs + test_path_pairs)
    for ipath, mpath in zip(img_paths, mask_paths):
        assert os.path.exists(ipath), \
            f'image file "{ipath}" is not exists'
        assert os.path.exists(mpath), \
            f'image file "{mpath}" is not exists'

    # example functions
    def nums_example(num_train, num_valid, num_test, num_class):
        feature = {
            'num_train': _int64_feature(num_train),
            'num_valid': _int64_feature(num_valid),
            'num_test':  _int64_feature(num_test),
            'num_class': _int64_feature(num_class)}
        return tf.train.Example(features=tf.train.Features(feature=feature))

    def colormap_example(src_dst_colormap):
        src_rgbs  = list(src_dst_colormap.keys())
        dst_1hots = list(src_dst_colormap.values())
        feature = {
            'src_rgbs': tf.train.Feature(
                int64_list=tf.train.Int64List(
                    value=fp.lmap(hex_rgb, src_rgbs))),
            'dst_1hots': tf.train.Feature(
                int64_list=tf.train.Int64List(
                    value=fp.lmap(bin_1hot, dst_1hots)))}
        return tf.train.Example(features=tf.train.Features(feature=feature))

    def datum_example(img_bin, mask_bin):
        h,w,c   = img_bin.shape
        mh,mw,_ = mask_bin.shape
        assert h == mh and w == mw, 'img.h,w = {} != {} mask.h,w'.format(
            img_bin.shape[:2], mask_bin.shape[:2])

        feature = {
            'h': _int64_feature(h),
            'w': _int64_feature(w),
            'c': _int64_feature(c),
            'mc': _int64_feature(len(src_dst_colormap)), # mask channels
            'img': _bytes_feature(img_bin.tobytes()),
            'mask': _bytes_feature(mask_bin.tobytes())}
        return tf.train.Example(features=tf.train.Features(feature=feature))

    # Create and save tfrecords dataset.
    with tf.io.TFRecordWriter(out_path) as writer:
        # 1. nums
        tf_example = nums_example(
            len(train_path_pairs), len(valid_path_pairs), len(test_path_pairs),
            len(src_dst_colormap))
        writer.write(tf_example.SerializeToString())
        # 2. colormap
        writer.write(colormap_example(src_dst_colormap).SerializeToString())
        # 3. image,mask pairs
        imgseq = fp.map(
            fp.pipe(cv2.imread, lambda im: (im / 255).astype(np.float32)), 
            img_paths)
        maskseq = fp.map(
            fp.pipe(
                cv2.imread, 
                map_colors(src_dst_colormap),
                lambda im: im.astype(np.float32)), 
            mask_paths)
        for img_bin, mask_bin in tqdm(zip(imgseq, maskseq), total=len(img_paths)):
            tf_example = datum_example(img_bin, mask_bin)
            writer.write(tf_example.SerializeToString())

@fp.multi
def export(connection, out_path, out_form, dataset, option=None):
    return out_form, dataset, option

@fp.mmethod(export, ('tfrecord', Dataset('old_snet', 'full'), 'rbk'))
def export(connection, out_path, out_form, dataset, option):
    export_old_snet(connection, out_path, dataset, option)
@fp.mmethod(export, ('tfrecord', Dataset('old_snet', 'full'), 'wk'))
def export(connection, out_path, out_form, dataset, option):
    export_old_snet(connection, out_path, out_form, dataset, option)
    
def export_old_snet(connection, out_path, dset, option):
    train,valid,test = 'train','valid','test'
    #path_pairs = dict(
    #)
    mask = Table('mask')
    dataset = Table('dataset')
    dataset_annotation = Table('dataset_annotation')
    train = db.get(
        Query.from_(dataset).from_(dataset_annotation).from_(mask)
             .select(dataset_annotation.input, dataset_annotation.output, mask.scheme)
             .where((dataset.name == dset.name) &
                    (dataset.split == dset.split) &
                    (dataset_annotation.output == mask.uuid) &
                    (mask.scheme == option)
             ),
        *connection
    )
    # TODO: Get biggest.
    print(train)
    for row in train:
        print(row.as_dict())
                 
        
