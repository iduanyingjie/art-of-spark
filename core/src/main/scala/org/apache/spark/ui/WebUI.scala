/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 *
 * @param securityManager SparkEnv中创建的安全管理器SecurityManager
 * @param sslOptions 使用SecurityManager获取spark.ssl.ui属性指定的WebUI的SSL选项
 * @param port WebUI对外服务的端口，可以使用spark.ui.port属性进行配置
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,
    val sslOptions: SSLOptions,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "")
  extends Logging {

  protected val tabs = ArrayBuffer[WebUITab]()
  // ServletContextHandler的缓冲数组。ServletContextHandler是jetty提供的API，
  // 负责对ServletContext进行处理。
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  // WebUIPage与ServletContextHandler缓冲数组之间的映射关系。
  // 由于WebUIPage的两个方法render和renderJson分别需要由一个对应的ServletContextHandler处理，
  // 所以一个WebUIPage对应两个ServletContextHandler。
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  // WebUI的Jetty服务器信息
  protected var serverInfo: Option[ServerInfo] = None
  // 当前WebUI的Jetty服务的主机名
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
  // 过滤了$符号的当前类的简单名称。
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
  def getTabs: Seq[WebUITab] = tabs.toSeq
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq
  def getSecurityManager: SecurityManager = securityManager

  /** Attach a tab to this UI, along with all of its attached pages. */
  def attachTab(tab: WebUITab) {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  def detachTab(tab: WebUITab) {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  /**
   * 作用与attachPage相反
   */
  def detachPage(page: WebUIPage) {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attach a page to this UI. */
  def attachPage(page: WebUIPage) {
    val pagePath = "/" + page.prefix
    // 首先调用工具类JettyUtils的createServletHandler方法给WebUIPage创建与render和renderJson两个方法
    // 分别关联的ServletContextHandler，然后通过attachHandler方法添加到handlers缓存数组中与Jetty服务器中，
    // 最后把WebUIPage与renderHandler这个ServletContextHandler的映射关系更新到pageToHandlers中。
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
  }

  /** Attach a handler to this UI. */
  def attachHandler(handler: ServletContextHandler) {
    // 给handlers数组中添加ServletContextHandler
    handlers += handler
    // 将ServletContextHandler通过ServerInfo的addHandler方法添加到Jetty服务器中。
    serverInfo.foreach(_.addHandler(handler))
  }

  /** Detach a handler from this UI. */
  def detachHandler(handler: ServletContextHandler) {
    // 从handlers数组中移除ServletContextHandler
    handlers -= handler
    // 将ServletContextHandler通过ServerInfo的removeHandler方法从Jetty服务器中移除。
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
   * Add a handler for static content.
   *
   * 首先调用工具类JettyUtils的createStaticHandler方法创建静态文件服务的ServletContextHandler，
   * 然后施加attachHandler方法。
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  def addStaticHandler(resourceBase: String, path: String): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /**
   * Remove a static content handler.
   *
   * @param path Path in UI to unmount.
   */
  def removeStaticHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /** Initialize all components of the server. */
  def initialize(): Unit

  /**
   * Bind to the HTTP server behind this web interface.
   * 启动与WebUI绑定的Jetty服务
   */
  def bind() {
    assert(!serverInfo.isDefined, s"Attempted to bind $className more than once!")
    try {
      val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
      serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name))
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  /** Return the url of web interface. Only valid after bind(). */
  def webUrl: String = s"http://$publicHostName:$boundPort"

  /** Return the actual port to which this server is bound. Only valid after bind(). */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind(). */
  def stop() {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.get.stop()
  }
}


/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
 *
 * 将多个页面作为一组内容放置在一起。
 *
 * @param parent 上一级节点，即父亲。WebUITab的父亲只能是WebUI
 * @param prefix 当前WebUITab的前缀，prefix将与上一级节点的路径一起构成当前WebUITab的访问路径。
 */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  // 当前WebUITab所包含的WebUIPage的缓冲数组。
  val pages = ArrayBuffer[WebUIPage]()
  // 当前WebUITab的名称，name实际上是将prefix的首字母转换成大写字母后取得
  val name = prefix.capitalize

  /**
   * Attach a page to this tab. This prepends the page's prefix with the tab's own prefix.
   *
   * 首先将当前WebUITab的前缀与WebUIPage的前缀拼接，
   * 作为WebUIPage的访问路径，然后向pages中添加WebUIPage
   */
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI. */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  def basePath: String = parent.getBasePath
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
 *
 * 表示UI层次结构中叶节点的页面。
 * 没有指定WebUIPage的直接父项，因为它可以是WebUI或WebUITab。
 * 如果父级是WebUI，则将前缀附加到父级地址以形成完整路径。
 * 否则，如果父级是WebUITab，则将前缀附加到父级的超级前缀以形成相对路径。
 * 前缀不能包含斜杠。
 *
 * WebUIPage在WebUI框架体系中的上一级节点（也可以称之为父亲）可以使WebUI或者WebUITab，
 * 其成员属性prefix将与上一级节点的路径一起构成当前WebUIPage的访问路径
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  /**
   * 渲染页面
   */
  def render(request: HttpServletRequest): Seq[Node]

  /**
   * 生成JSON
   */
  def renderJson(request: HttpServletRequest): JValue = JNothing
}
