if __name__ == "__main__":
    import asyncio

    from infrastructure.rabbitmq import config
    
    from actions.filtersuccessresponse import FilterSuccessResponseHandler
    from actions.filterhtmlresponse import FilterHtmlResponseHandler
    from actions.filternewdata.handler import FilterNewDataHandler
    from actions.getcontentfromhtml import GetContentFromHtmlHandler
    from actions.getlinksfromhtml import GetLinksFromHtmlHandler
    from actions.requesturl import RequestUrlHandler

    from customactionhandler import create_custom_action_registration_handler

    app = config.create_faststream_app()
    register_custom_action = create_custom_action_registration_handler(config.run_action, config.action_handler)
    register_custom_action(RequestUrlHandler())
    register_custom_action(FilterSuccessResponseHandler())
    register_custom_action(FilterHtmlResponseHandler())
    register_custom_action(GetContentFromHtmlHandler())
    register_custom_action(GetLinksFromHtmlHandler())
    register_custom_action(FilterNewDataHandler())

    asyncio.run(app.run())