if __name__ == "__main__":
    import asyncio
    
    from actions.filtersuccessresponse import FilterSuccessResponseHandler
    from actions.filterhtmlresponse import FilterHtmlResponseHandler
    from actions.filternewdata.handler import FilterNewDataHandler
    from actions.getcontentfromhtml import GetContentFromHtmlHandler
    from actions.getlinksfromhtml import GetLinksFromHtmlHandler
    from actions.requesturl import RequestUrlHandler
    from actions.sendtoviberchannel.handler import SendToViberChannelHandler

    from config import action_handler, app, run_action, viber_api_config
    from customactionhandler import create_custom_action_registration_handler

    register_custom_action = create_custom_action_registration_handler(run_action, action_handler)
    register_custom_action(RequestUrlHandler())
    register_custom_action(FilterSuccessResponseHandler())
    register_custom_action(FilterHtmlResponseHandler())
    register_custom_action(GetContentFromHtmlHandler())
    register_custom_action(GetLinksFromHtmlHandler())
    register_custom_action(FilterNewDataHandler())
    register_custom_action(SendToViberChannelHandler(viber_api_config))

    asyncio.run(app.run())