#include "runtime/runtime_filter_mgr.h"

#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/plan_fragment_executor.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeFilterMgrTest : public testing::Test {
public:
    RuntimeFilterMgrTest() = default;
    ~RuntimeFilterMgrTest() = default;
    virtual void SetUp() {
        _executor.reset(new PlanFragmentExecutor(
                ExecEnv::GetInstance(),
                [](const Status& status, RuntimeProfile* profile, bool done) {}));
    }
    virtual void TearDown() {}

private:
    std::unique_ptr<PlanFragmentExecutor> _executor;
};

TEST_F(RuntimeFilterMgrTest, init_case) {}

} // namespace doris