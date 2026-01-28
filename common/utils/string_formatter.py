class StringFormatter:
    """字符串通配符替换工具类"""
    
    @staticmethod
    def format(template: str, *args) -> str:
        """
        替换字符串中的 %s 通配符
        
        Args:
            template: 包含 %s 通配符的模板字符串
            *args: 用于替换的参数，数量不限
            
        Returns:
            替换后的字符串
            
        Example:
            >>> StringFormatter.format("Hello %s, you are %s years old", "Alice", 25)
            'Hello Alice, you are 25 years old'
        """
        return template % args if args else template
    
    @staticmethod
    def format_dict(template: str, **kwargs) -> str:
        """
        使用命名参数替换字符串中的 %(name)s 通配符
        
        Args:
            template: 包含 %(name)s 通配符的模板字符串
            **kwargs: 用于替换的命名参数
            
        Returns:
            替换后的字符串
            
        Example:
            >>> StringFormatter.format_dict("Hello %(name)s, age %(age)s", name="Bob", age=30)
            'Hello Bob, age 30'
        """
        return template % kwargs if kwargs else template
