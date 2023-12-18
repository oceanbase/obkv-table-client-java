# Notice
- We do not support across partition in batch operation.
- Batch operations is atomic. When one of the operation in a batch execute failed, the batch operation will be rollbacked.
- More details are shown in [Demo](https://github.com/oceanbase/obkv-table-client-java/blob/master/example/simple-table-demo/src/main/java/com/oceanbase/example/TableClient.java) `batch2` and `batch3`