library('clv')
library('amap')
path = '/home/nilm/Desktop/cluster_tsai'
df = read.csv(file= paste(path, '/data/max_min_sum_w_dataSet.csv', sep=''), header=TRUE)
df = df[,c(4:99)]
PCA = princomp(df, cor = TRUE)
new = as.data.frame(predict(PCA, newdata = df)[, 1:24]) #
#### calculate the stability for multiple index ####
#### large is better ####
fu  = function(data, clust.num) Kmeans(data , clust.num, nstart=25, iter.max=1000)$clust ## Kmeans
stability = cls.stab.sim.ind.usr(new, c(3:10), fu, sim.ind.type=c('dot.pr','sim.ind','rand','jaccard'), rep.num=10, subset.ratio=0.8)
#### calculate the DBI ####
intraclust = c('average') # 'complete', 'centroid'
interclust = c('average') # 'complete', 'centroid'
Dav = matrix(0, nrow=1,ncol=10)
#### calculate the DBI  ####
#### smaller is better ####
for (c in 3:10){
  K = Kmeans(new , c, nstart=25, iter.max=1000)
  # Intra-cluster variance
  #SCATT[c] = clv.Scatt(new, K$cluster)$Scatt
  # DBI, Dunn
  cls.scatt = cls.scatt.data(new, K$cluster)
  Dav[c] =  clv.Davies.Bouldin(cls.scatt, intraclust, interclust)
}
#### merge the result #####
result = c( which.max(colMeans(stability$dot.pr))+2, 
            which.max(colMeans(stability$sim.ind))+2, 
            which.max(colMeans(stability$rand))+2, 
            which.max(colMeans(stability$jaccard))+2, 
            which.min(Dav[c(3:10)]) + 2  )
#### choose the mode as our result ####s
uniqv = unique(result)
cluster = uniqv[which.max(tabulate(match(result,uniqv)))]
write.csv(cluster, file=paste(path,'/DataSet/cluster_num.csv',sep=''))

