pa.hist = read.csv("result/histogram",header = FALSE,sep = "\t", col.names = c("length", "count"))
pa.histp <-  pa.hist[order(pa.hist$length), ]
pa.histp$length <- pa.histp$length * 5
pa.histpb <- pa.histp[pa.histp$length >= 150,]
pa.histps <- pa.histp[pa.histp$length < 135,]

ggplot(data=pa.histp, aes(x=length, y=count), colors = count) + 
  geom_bar(stat="identity", fill="steelblue") + 
  labs(x = "Length", y = "Count", title = "Tweets Length Analysis")

ggplot(data=pa.histpb, aes(x=length, y=count), colors = count) + 
  geom_bar(stat="identity", fill="steelblue") + 
  labs(x = "Length", y = "Count", title = "Tweets Length Analysis")

ggplot(data=pa.histps, aes(x=length, y=count), colors = count) + 
  geom_bar(stat="identity", fill="steelblue") + 
  labs(x = "Length", y = "Count", title = "Tweets Length Analysis")
