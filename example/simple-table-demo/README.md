# Notice
- We cannot ensure atomicity when a batch has operations across tablets/partitions,
  only ensure atomicity for operations on the same partition. More details are shown in [Demo](https://github.com/oceanbase/obkv-table-client-java/blob/master/example/simple-table-demo/src/main/java/com/oceanbase/example/TableClient.java) `batch2` and `batch3`