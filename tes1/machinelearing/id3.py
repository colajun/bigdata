#_*_encoding:utf-8_*_
import  math


dataset = [
    ("青年", "否", "否", "一般", "否")
    ,("青年", "否", "否", "好", "否")
    ,("青年", "是", "否", "好", "是")
    ,("青年", "是", "是", "一般", "是")
    ,("青年", "否", "否", "一般", "否")
    ,("中年", "否", "否", "一般", "否")
    , ("中年", "否", "否", "好", "否")
    , ("中年", "是", "是", "好", "是")
    , ("中年", "否", "是", "非常好", "是")
    , ("中年", "否", "是", "非常好", "是")
    ,("老年", "否", "是", "非常好", "是")
    ,("老年", "否", "是", "好", "是")
    ,("老年", "是", "否", "好", "是")
    ,("老年", "是", "否", "非常好", "是")
    ,("老年", "否", "否", "一般", "否")
]

labels =["var1", "var2", "var3", "var4"]


data = [[1, 1, "yes"],
        [1, 1, "yes"],
        [1,0, "no"],
        [0, 1, "no"],
        [0, 1, "no"]]
la = ["no surfacing", "flippes"]



#计算信息商， target为分类标签的位置
def entry(dataset, target=-1):
    instance_num = float(len(dataset))
    tar_num = {}
    entro = 0.0
    for t in dataset:
        tar_num[t[target]]= tar_num.setdefault(t[target], 0)+1
    pl = [n/instance_num for n in tar_num.values()]
    for p in pl:
        entro -= p*math.log(p, 2)
    return entro


#拆分数据集
def split_dataset(dataset, f_index, value):
    sub_set = []
    for row in  dataset:
        row = list(row)
        if row[f_index] == value:#如果在数据集dataset的每一个数据记录里面的分类属性等于判断的值value，就把这个判断值从记录当中去掉，后面得到的子集没有这个属性
            row.remove(row[f_index])
            sub_set.append(row)
    return  sub_set


###计算信息增益
def info_gain(dataset, feature_index):
    entropy_d = entry(dataset)
    long_dataset = len(dataset)
    feature_count = {}

    sub_entropy = 0.0
    for row in dataset:
        #计算信息增益，使用特征的索引得到具体特征值的字典，即是从某个特征将记录分类，得到的某一类的个数和特在值放在字典feature_count里面
        feature_count[row[feature_index]] = feature_count.setdefault(row[feature_index], 0)+1
    feature_p = 0.0

    for k, v in feature_count.items():
        feature_p = v / long_dataset
        subset = split_dataset(dataset, feature_index, k)#调用split_dataset方法来使用某个特征索引对应的其中的某个具体值得到划分的子集
        sub_entropy += feature_p * entry(subset)#得到按照某个特征的某个具体划分子集的信息商，乘占的比值feature_p

    ig = entropy_d - sub_entropy
    #print(ig)
    return ig#返回数据集按照某个特征划分获得的信息增益



##寻找最大信息增熹的特在
def find_bestfeature(dataset, label):#函数的参数是数据集和数据记录里面的标签
    max_ig = 0.0

    for la in label:#对于每一个数据记录的 特在，依次计算信息增益
        label_index = label.index(la)
        ig = info_gain(dataset, label_index)
        if ig > max_ig:
            max_ig  = ig
            baset_feature = la
    return  (baset_feature, label_index)#得到最大信息增益的特征和特在的索引

###返回数量最多的value
def max_count(dataset):
    clas_count= {}
    if dataset[0] == 1:
        for row in dataset:
           clas_count[row[0]] = clas_count.setdefault(row[0], 0)+1
        sorted_count = sorted(clas_count.items(), reverse=True)
        return  sorted_count[0][0]
    else:
        return None


###create_tree
def create_tree(dataset, label):
    class_list=[x[-1] for x in dataset]  #数据集dataset有哪些类别
    if class_list.count(class_list[0]) == len(dataset):#如果只有一种类别
        return  (class_list[0])
    if len(dataset) == 1:#如果数据集只有一条记录
        return (max_count(dataset))

    best_feature, index = find_bestfeature(dataset, label)
    tree ={best_feature:{}}#第归的决策树的创建
    feature_list = set(x[index] for x in dataset)#得到最好的特征的索引处的不同值,
    label.remove(best_feature)#将划分的特在从原来的特征里面移除
    for ft in feature_list:#对于每一个具体的特征ft，又从此处得到的树开始划分，第归开始
        sublabel = label[:]#得到所有的子特征
        subset = split_dataset(dataset, index, ft)
        tree[best_feature][ft] = create_tree(subset, sublabel)#第归调用树
    return  tree



def classfy(inputTree, featurelabels, testVec):#分类的算法开始,参数是已经得到的树，特征的标签和要进行分类的记录
    firstStr = list(inputTree.keys())[0]#得到第一个树分支的判断值key
    secondDict = inputTree[firstStr]
    featIndex = featurelabels.index(firstStr)
    for key in secondDict.keys():
        if testVec[featIndex] == key:
            if type(secondDict[key]).__name__ == "dict":
                classLabel = classfy(secondDict[key], featurelabels, testVec)
            else:
                classLabel = secondDict[key]
    return  classLabel

mytree = create_tree(data, la)
test = classfy(mytree, ["no surfacing", "flippes"], [0, 1])
print test



























