import molexp as me

class TestProject:

    def test_init_exp(self):

        import func

        proj = me.Project("test_exp")
        proj.add_tasks(func)
        proj.execute(
            {'params': me.ParamSpace({
                'sleep_time': [1, 2],
                'b': [3, 4]
            })}, 
            final_vars=["post_process"]
        )
        assert False