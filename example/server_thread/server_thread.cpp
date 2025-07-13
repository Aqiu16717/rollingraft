#include <rollingraft/server.h>
#include <vector>
#include <thread>

int main()
{
#if 0
    std::vector<uint32_t> peers{9527,9528,9529};
    std::thread t1([&](){
        rollingraft::Server s1(1,9527,peers);
        s1.Start();
    });
    std::thread t2([&](){
        rollingraft::Server s2(1,9528,peers);
        s2.Start();
    });
    std::thread t3([&](){
        rollingraft::Server s3(1,9529,peers);
        s3.Start();
    });

    t1.join();
    t2.join();
    t3.join();
#endif
    return 0;
}