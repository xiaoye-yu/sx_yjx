

class Solution:
    def twoSum(self, nums, target):
        """
        :type nums: List[int]
        :type target: int
        :rtype: List[int]
        """

        #遍历一个i,就把nums[i]放在字典里。之后i不断相加，总会碰到
        #target - nums[i]在字典里的情况。碰到的时候return即可。这里注意到先return的是dict[a],再是当前的i,原因就是dict里存的value都是以前遍历过的i,所以它比当前的i小，按从小到大的顺序所以这样排。
        dict = {}
        for i in range(len(nums)):
            a = target - nums[i]
            if a in dict:
                return dict[a],i
            else:
                dict[nums[i]] = i

nums = [2, 7, 11, 15]
target = 9
print(Solution().twoSum(nums, target))
