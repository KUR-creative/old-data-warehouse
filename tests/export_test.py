import cv2
import funcy as F
from bidict import bidict
import tensorflow as tf
import numpy as np

def unique_colors(img):
    return np.unique(img.reshape(-1,img.shape[2]), axis=0)

def unique_color_set(img):
    return set(map( tuple, unique_colors(img).tolist() ))

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

def tup_rgb(hex_rgb):
    ''' hex rgb -> tuple rgb '''
    assert 0 <= hex_rgb <= 0xFFFFFF, hex_rgb
    return (
        (hex_rgb & 0xFF0000) >> (8 * 2),
        (hex_rgb & 0x00FF00) >> (8 * 1),
        (hex_rgb & 0x0000FF) >> (8 * 0))

@F.autocurry
def tup_1hot(num_class, bin_1hot):
    ''' one-hot binary(int) -> one-hot (tuple) '''
    assert 0 < bin_1hot <= 2**(num_class - 1), \
        'assert fail: 0 < {} <= {}'.format(
            bin_1hot, 2**(num_class - 1))
    assert (bin_1hot % 2 == 0 or bin_1hot == 1), \
        'assert fail: {} % 2 == 0 or {} == 1'.format(bin_1hot)

    ret = [0] * num_class
    for i in range(num_class + 1):
        if bin_1hot >> i == 0:
            ret[-i] = 1
            return tuple(ret)

def read(dset_kind, tfrecord_dset):
    '''
    Load(read) tfrecord_dset to dict
    '''
    def parse_nums(example):
        return tf.io.parse_single_example(
            example,
            {'num_train': tf.io.FixedLenFeature([], tf.int64),
             'num_valid': tf.io.FixedLenFeature([], tf.int64),
             'num_test':  tf.io.FixedLenFeature([], tf.int64),
             'num_class': tf.io.FixedLenFeature([], tf.int64)})
    def parse_colormap(num_class, example):
        n = num_class
        return tf.io.parse_single_example(
            example,
            {'src_rgbs':  tf.io.FixedLenFeature([n], tf.int64, [-1]*n),
             'dst_1hots': tf.io.FixedLenFeature([n], tf.int64, [-1]*n)})
    def parse_im_pair(example):
        return tf.io.parse_single_example(
            example,
            {'h': tf.io.FixedLenFeature([], tf.int64),
             'w': tf.io.FixedLenFeature([], tf.int64),
             'c': tf.io.FixedLenFeature([], tf.int64),
             'mc': tf.io.FixedLenFeature([], tf.int64),
             'img': tf.io.FixedLenFeature([], tf.string),
             'mask': tf.io.FixedLenFeature([], tf.string)})

    for no, example in enumerate(tfrecord_dset): 
        if no == 0:
            datum = parse_nums(example)
            num_train = datum['num_train'].numpy()
            num_valid = datum['num_valid'].numpy()
            num_test  = datum['num_test'].numpy()
            num_class = datum['num_class'].numpy()
        elif no == 1:
            datum = parse_colormap(num_class, example)
            src_rgbs  = datum['src_rgbs'].numpy().tolist()
            dst_1hots = datum['dst_1hots'].numpy().tolist()
            src_dst_colormap = bidict(F.zipdict(
                map(tup_rgb, src_rgbs),
                map(tup_1hot(num_class), dst_1hots)))
        else:
            break

    train_pairs =(tfrecord_dset.skip(2)
                               .take(num_train).map(parse_im_pair))
    valid_pairs =(tfrecord_dset.skip(2 + num_train)
                               .take(num_valid).map(parse_im_pair))
    test_pairs  =(tfrecord_dset.skip(2 + num_train + num_valid)
                               .take(num_test).map(parse_im_pair))
    return dict(
        cmap  = src_dst_colormap,
        train = train_pairs,
        valid = valid_pairs,
        test  = test_pairs,
        num_train = num_train,
        num_valid = num_valid,
        num_test  = num_test,
        num_class = num_class)

@tf.function
def decode_raw(str_tensor, shape, dtype=tf.float32):
    ''' Decode str_tensor(no type) to dtype(defalut=tf.float32). '''
    return tf.reshape(tf.io.decode_raw(str_tensor, dtype), shape)

@tf.function(experimental_relax_shapes=True) 
def crop(img, mask, size):
    ''' Crop img and mask in (size, size) at same (y,x). '''
    h = tf.shape(img)[0]
    w = tf.shape(img)[1]
    #assert (h,w,1) == mask.shape

    max_x = w - size
    max_y = h - size
    #assert max_x > size

    x = tf.random.uniform([], maxval=max_x, dtype=tf.int32)
    y = tf.random.uniform([], maxval=max_y, dtype=tf.int32)

    #return (img[y:y+size, x:x+size], mask[y:y+size, x:x+size])
    return (tf.image.crop_to_bounding_box(img, y,x, size,size),
            tf.image.crop_to_bounding_box(mask, y,x, size,size))
    #return (img, mask)

def tmp_dset_testing(expected_dset_path):
    #expected_dset_path = '/home/kur/dev/szmc/nn-lab/dataset/snet285wk.tfrecords'
    #expected_dset_path = '/home/kur/dev/szmc/nn-lab/dataset/snet285rbk.tfrecords'
    dset = read('old_snet', tf.data.TFRecordDataset(expected_dset_path))
    
    # Train
    train_pairs = dset["train"]
    src_dst_colormap = dset["cmap"]
    n_train = dset["num_train"]
    
    BATCH_SIZE = 4
    EPOCHS = 2
    IMG_SIZE = 700
    
    @tf.function
    def crop_datum(datum):
        h  = datum["h"]
        w  = datum["w"]
        c  = datum["c"]
        mc = datum["mc"]
        return (
            decode_raw(datum["img"], (h,w,c)),
            decode_raw(datum["mask"], (h,w,mc))
        )

        '''
        return crop(
            decode_raw(datum["img"], (h,w,c)),
            decode_raw(datum["mask"], (h,w,mc)), 
            IMG_SIZE)
        '''

    seq = enumerate(
        dset["train"]
            #.take(4)
            .shuffle(n_train, reshuffle_each_iteration=True)
            .map(crop_datum, tf.data.experimental.AUTOTUNE)
            #.batch(BATCH_SIZE)
            .repeat(EPOCHS)
            .prefetch(tf.data.experimental.AUTOTUNE), 
        start=1)
    
    for step, (img_batch, mask_batch) in seq:
        # Look and Feel check!
        print('step:',step)
        #print(tf.shape(img_batch), tf.shape(mask_batch))
        #print(img_batch.dtype, mask_batch.dtype)
        
        '''
        cv2.imshow("i", img_batch.numpy())
        cv2.imshow("m", mask_batch.numpy())
        cv2.waitKey(0)
        print('->', len(img_batch), len(mask_batch))
        
        #print(img_batch.numpy().shape)
        #print(mask_batch.numpy().shape)
        '''
        print(step)
        img, mask = img_batch.numpy(), mask_batch.numpy()
        #mapped_mask = mask
        mapped_mask = map_colors(src_dst_colormap.inverse, mask)
        cv2.imshow("i", img)
        cv2.imshow("m", mapped_mask)
        cv2.waitKey(0)
        '''
        for i in range(len(img_batch)):
            
            print('i:',i)
            img, mask = img_batch[i].numpy(), mask_batch[i].numpy()
            #mapped_mask = mask
            mapped_mask = map_colors(src_dst_colormap.inverse, mask)
            cv2.imshow("i", img)
            cv2.imshow("m", mapped_mask)
            cv2.waitKey(0)
            #print(unique_colors(mask))
        '''

def test_export():
    # for pytest
    tmp_dset_testing('/home/kur/dev/szmc/nn-lab/dataset/snet285wk.tfrecords')
            
if __name__ == '__main__':
    tmp_dset_testing('/home/kur/dev/szmc/nn-lab/dataset/snet285wk.tfrecords')
