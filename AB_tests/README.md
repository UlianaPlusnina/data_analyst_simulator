# A/B tests
1. AA_test

Does the splitting system work correctly? 
We'd like to ensure that our metric doesn't differ between the groups. We have to repeatedly extract sub-samples with repetitions from our data, then conduct t-test. And at the end we will see in what percentage of cases we rejected the H0.
**result:**
The key metric doesn't statistically significant differ between the groups, so our groups are identical.

2. AB_test

Hypothesis: the new algorithm in group 2 will increase the CTR. To test this hypothesis, it is necessary to conduct A / B tests.
**result:**
We can reject the hypothesis that the new algorithm in group 2 will increase CTR. I don't recommend implementing a new algorithm.
  
3. AB_test_with_linearized_metric

a) Analyze the test between groups 0 and 3 with linearized likes metric. Is there a difference? Has p-value become smaller?
b) Analyze the test between groups 1 and 2 with linearized likes metric. Is there a difference? Has p-value become smaller?
**result:**
The linearized likes metric is much more sensitive than CTR. It even shows statistically significant difference between groups 1 and 2.