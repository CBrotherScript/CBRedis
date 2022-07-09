# CBRedis
这是一个CBrother的redis扩展库

# 源码编译后安装
1. 编译后的CBGdImage动态库放到CBrother目录下

    win_x86 : ext\CBRedis\win\x86\CBRedis.dll    
    win_x64 : ext\CBRedis\win\amd64\CBRedis.dll    
    linux_x86 : ext\CBRedis\linux\amd64\CBRedis.so    
    linux_x64 : ext\CBRedis\linux\x86\CBRedis.so    
    macos_x64 : ext\CBRedis\mac\amd64\CBRedis.dylib
    3. lib/redis.cb和sample/redis_test.cb放到CBrother对应目录下
    
    
## 安装成功后使用的例子
```javascript

import lib/redis
function main(parm)
{
    var client = new redis::Client();
    if(!client.connect("127.0.0.1",6379,5000))
    {
        print "connect err!";
        return;
    }

    client.select(0);

    client.set("kk",123);
    print client.get("kk");
}
```
