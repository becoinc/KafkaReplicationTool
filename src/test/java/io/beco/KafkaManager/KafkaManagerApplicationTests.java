package io.beco.KafkaManager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka( topics = { "test.topics.1", "test.topics.2" } )
@SpringBootTest( webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
                 classes = { KafkaManagerApplication.class, TestConfiguration.class } )
public class KafkaManagerApplicationTests
{

    private static final String TEST_TOPIC_1 = "test.topics.1";
    private static final String TEST_TOPIC_2 = "test.topics.2";

    @LocalServerPort
    private int port;

    @Autowired private KafkaEmbedded kafkaEmbedded;

    @Autowired
    private WebApplicationContext mWebAppCtxt;

    private String baseUrl;

    private MockMvc mMockMvc;

    @Before
    public void setup()
    {
        this.baseUrl = String.format( "https://localhost:%d/", this.port );
        mMockMvc =
            MockMvcBuilders
                .webAppContextSetup( mWebAppCtxt )
                //.apply( SecurityMockMvcConfigurers.springSecurity() )
                .build();
    }

    @Test
    public void contextLoads() throws Exception
    {
        final UriComponentsBuilder uri
            = UriComponentsBuilder.fromUriString( this.baseUrl );
                                  //.queryParam( "customerId", admin.getCustomerId() )
                                  //.pathSegment( "internal", "data" );

        final MockHttpServletRequestBuilder builder
            = MockMvcRequestBuilders.get( uri.toUriString() )
        // .with( SecurityMockMvcRequestPostProcessors.httpBasic( username, password ) )
            ;

        final MvcResult result
            = mMockMvc.perform( builder )
                      .andDo( MockMvcResultHandlers.print() )
                      .andExpect( MockMvcResultMatchers.status().isOk() )
                      .andExpect( MockMvcResultMatchers.content().contentTypeCompatibleWith( MediaType.TEXT_HTML ) )
                      // xpath uses 1-based indexes
                      .andExpect( MockMvcResultMatchers.xpath( "/html/body" ).exists() )
                      .andReturn()
        ;
        final ModelAndView mv = result.getModelAndView();
        final Map< String, Object > model = mv.getModel();
        Assert.assertTrue( "Must contain test topic names",    model.containsKey( "topicNames" ) );
        Assert.assertTrue( "Must contain test topic listings", model.containsKey( "topicListings" ) );
        Assert.assertTrue( "Must contain test cluster id",     model.containsKey( "clusterId" ) );
        Assert.assertTrue( "Must contain test nodes",          model.containsKey( "nodes" ) );
        Assert.assertTrue( "Must contain test controller",     model.containsKey( "controller" ) );
        final Set< String > topicNames = ( Set< String > ) model.get( "topicNames" );
        Assert.assertTrue( "Must contain test topics", topicNames.contains( TEST_TOPIC_1 ) );
        Assert.assertTrue( "Must contain test topics", topicNames.contains( TEST_TOPIC_2 ) );
    }

    @Test
    public void testGetTopicInfo() throws Exception
    {
        final UriComponentsBuilder uri
            = UriComponentsBuilder.fromUriString( this.baseUrl )
                                  .pathSegment( "topic", TEST_TOPIC_1, "describe" );

        final MockHttpServletRequestBuilder builder
            = MockMvcRequestBuilders.get( uri.toUriString() )
            // .with( SecurityMockMvcRequestPostProcessors.httpBasic( username, password ) )
            ;

        final MvcResult result
            = mMockMvc.perform( builder )
                      .andDo( MockMvcResultHandlers.print() )
                      .andExpect( MockMvcResultMatchers.status().isOk() )
                      .andExpect( MockMvcResultMatchers.content().contentTypeCompatibleWith( MediaType.TEXT_HTML ) )
                      // xpath uses 1-based indexes
                      .andExpect( MockMvcResultMatchers.xpath( "/html/body" ).exists() )
                      .andReturn()
            ;
        final ModelAndView mv = result.getModelAndView();
        final Map< String, Object > model = mv.getModel();
        Assert.assertTrue( "Must contain test topic name", model.containsKey( "topicName" ) );
        Assert.assertTrue( "Must contain test nodes",      model.containsKey( "nodes" ) );
        Assert.assertEquals( model.get( "topicName" ), TEST_TOPIC_1 );
    }

}
