# bypass shuffle sort #

bypass shuffle sort 只能保证partition的顺序，而不能保证key的顺序。它的 原理很简单，对于每个record，根据key分配不同的partition文件，每个partition单独对应着一个文件。最后将partition文件合并成一个大的文件。