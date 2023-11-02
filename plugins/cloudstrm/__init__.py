import datetime
import os
import shutil
from pathlib import Path

import pytz
from typing import Any, List, Dict, Tuple

from app.core.config import settings
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from app.log import logger
from app.plugins import _PluginBase


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """

    def __init__(self, watching_path: str, file_change: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = watching_path
        self.file_change = file_change

    def on_any_event(self, event):
        logger.info(f"目录监控event_type {event.event_type} 路径 {event.src_path}")

    def on_created(self, event):
        self.file_change.event_handler(event=event, mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.file_change.event_handler(event=event, mon_path=self._watch_path, event_path=event.dest_path)


class CloudStrm(_PluginBase):
    # 插件名称
    plugin_name = "云盘strm生成"
    # 插件描述
    plugin_desc = "监控文件创建，生成strm文件。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/thsrite/MoviePilot-Plugins/main/icons/cloudstrm.png"
    # 主题色
    plugin_color = "#999999"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "thsrite"
    # 作者主页
    author_url = "https://github.com/thsrite"
    # 插件配置项ID前缀
    plugin_config_prefix = "cloudstrm_"
    # 加载顺序
    plugin_order = 26
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _monitor_confs = []
    _onlyonce = False
    _observer = []
    _video_formats = ('.mp4', '.avi', '.rmvb', '.wmv', '.mov', '.mkv', '.flv', '.ts', '.webm', '.iso', '.mpg')

    _dirconf = {}
    _modeconf = {}
    _libraryconf = {}

    def init_plugin(self, config: dict = None):
        # 清空配置
        self._dirconf = {}
        self._modeconf = {}
        self._libraryconf = {}

        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._monitor_confs = config.get("monitor_confs")

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 读取目录配置
            monitor_confs = self._monitor_confs.split("\n")
            if not monitor_confs:
                return
            for monitor_conf in monitor_confs:
                # 格式 源目录:目的目录:媒体库内网盘路径:监控模式
                if not monitor_conf:
                    continue
                if str(monitor_conf).count(":") != 3:
                    logger.error(f"{monitor_conf} 格式错误")
                    continue
                source_dir = str(monitor_conf).split(":")[0]
                target_dir = str(monitor_conf).split(":")[1]
                library_dir = str(monitor_conf).split(":")[2]
                mode = str(monitor_conf).split(":")[3]
                # 存储目录监控配置
                self._dirconf[source_dir] = target_dir
                self._libraryconf[source_dir] = library_dir
                self._modeconf[source_dir] = mode

                # 启用目录监控
                if self._enabled:
                    # 检查媒体库目录是不是下载目录的子目录
                    try:
                        if target_dir and Path(target_dir).is_relative_to(Path(source_dir)):
                            logger.warn(f"{target_dir} 是下载目录 {source_dir} 的子目录，无法监控")
                            self.systemmessage.put(f"{target_dir} 是下载目录 {source_dir} 的子目录，无法监控")
                            continue
                    except Exception as e:
                        logger.debug(str(e))
                        pass

                    try:
                        if str(mode) == "compatibility":
                            # 兼容模式，目录同步性能降低且NAS不能休眠，但可以兼容挂载的远程共享目录如SMB
                            observer = PollingObserver(timeout=10)
                        else:
                            # 内部处理系统操作类型选择最优解
                            observer = Observer(timeout=10)
                        self._observer.append(observer)
                        observer.schedule(FileMonitorHandler(source_dir, self), path=source_dir, recursive=True)
                        observer.daemon = True
                        observer.start()
                        logger.info(f"{source_dir} 的云盘监控服务启动")
                    except Exception as e:
                        err_msg = str(e)
                        if "inotify" in err_msg and "reached" in err_msg:
                            logger.warn(
                                f"云盘监控服务启动出现异常：{err_msg}，请在宿主机上（不是docker容器内）执行以下命令并重启："
                                + """
                                                     echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
                                                     echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf
                                                     sudo sysctl -p
                                                     """)
                        else:
                            logger.error(f"{source_dir} 启动云盘监控失败：{err_msg}")
                        self.systemmessage.put(f"{source_dir} 启动云盘监控失败：{err_msg}")

            # 运行一次定时服务
            if self._onlyonce:
                logger.info("云盘监控服务启动，立即运行一次")
                self._scheduler.add_job(func=self.sync_all, trigger='date',
                                        run_date=datetime.datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                        )
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

    def event_handler(self, event, source_dir: str, event_path: str):
        """
        处理文件变化
        :param event: 事件
        :param source_dir: 监控目录
        :param event_path: 事件文件路径
        """
        # 回收站及隐藏的文件不处理
        if (event_path.find("/@Recycle") != -1
                or event_path.find("/#recycle") != -1
                or event_path.find("/.") != -1
                or event_path.find("/@eaDir") != -1):
            logger.info(f"{event_path} 是回收站或隐藏的文件，跳过处理")
            return

        # 文件发生变化
        logger.info(f"变动类型 {event.event_type} 变动路径 {event_path}")
        self.__handle_file(event=event, event_path=event_path, source_dir=source_dir)

    def __handle_file(self, event, event_path: str, source_dir: str):
        """
        同步一个文件
        :param event_path: 事件文件路径
        :param source_dir: 监控目录
        """
        try:
            # 转移路径
            dest_dir = self._dirconf.get(source_dir)
            # 媒体库容器内挂载路径
            library_dir = self._libraryconf.get(source_dir)
            # 文件夹同步创建
            if event.is_directory:
                target_path = event_path.replace(source_dir, dest_dir)
                # 目标文件夹不存在则创建
                if not Path(target_path).exists():
                    logger.info(f"创建目标文件夹 {target_path}")
                    os.makedirs(target_path)
            else:
                # 文件：nfo、图片、视频文件
                dest_file = event_path.replace(source_dir, dest_dir)

                # 目标文件夹不存在则创建
                if not Path(dest_file).parent.exists():
                    logger.info(f"创建目标文件夹 {Path(dest_file).parent}")
                    os.makedirs(Path(dest_file).parent)

                # 视频文件创建.strm文件
                if event_path.lower().endswith(self.__video_formats):
                    # 如果视频文件小于1MB，则直接复制，不创建.strm文件
                    if os.path.getsize(event_path) < 1024 * 1024:
                        shutil.copy2(event_path, dest_file)
                        logger.info(f"复制视频文件 {event_path} 到 {dest_file}")
                    else:
                        # 创建.strm文件
                        self.__create_strm_file(dest_file=dest_file,
                                                dest_dir=dest_dir,
                                                library_dir=library_dir)
                else:
                    # 其他nfo、jpg等复制文件
                    shutil.copy2(event_path, dest_file)
                    logger.info(f"复制其他文件 {event_path} 到 {dest_file}")

        except Exception as e:
            logger.error(f"event_handler_created error: {e}")
            print(str(e))

    def sync_all(self):
        """
        同步所有文件
        """
        if not self._dirconf or not self._dirconf.keys():
            logger.error("未获取到可用目录监控配置，请检查")
            return
        for source_dir in self._dirconf.keys():
            dest_dir = self._dirconf.get(source_dir)
            library_dir = self._libraryconf.get(source_dir)

            logger.info(f"开始初始化生成strm文件 {source_dir}")
            self.__handle_all(source_dir=source_dir,
                              dest_dir=dest_dir,
                              library_dir=library_dir)
            logger.info(f"{source_dir} 初始化生成strm文件完成")

    def __handle_all(self, source_dir, dest_dir, library_dir):
        """
        遍历生成所有文件的strm
        """
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        for root, dirs, files in os.walk(source_dir):
            # 如果遇到名为'extrafanart'的文件夹，则跳过处理该文件夹，继续处理其他文件夹
            if "extrafanart" in dirs:
                dirs.remove("extrafanart")

            for file in files:
                source_file = os.path.join(root, file)
                logger.info(f"处理源文件::: {source_file}")

                dest_file = os.path.join(dest_dir, os.path.relpath(source_file, source_dir))
                logger.info(f"开始生成目标文件::: {dest_file}")

                # 创建目标目录中缺少的文件夹
                if not os.path.exists(Path(dest_file).parent):
                    os.makedirs(Path(dest_file).parent)

                # 如果目标文件已存在，跳过处理
                if os.path.exists(dest_file):
                    logger.warn(f"文件已存在，跳过处理::: {dest_file}")
                    continue

                if file.lower().endswith(self._video_formats):
                    # 如果视频文件小于1MB，则直接复制，不创建.strm文件
                    if os.path.getsize(source_file) < 1024 * 1024:
                        logger.info(f"视频文件小于1MB的视频文件到:::{dest_file}")
                        shutil.copy2(source_file, dest_file)
                    else:
                        # 创建.strm文件
                        self.__create_strm_file(dest_file, dest_dir, library_dir)
                else:
                    # 复制文件
                    logger.info(f"复制其他文件到:::{dest_file}")
                    shutil.copy2(source_file, dest_file)

    @staticmethod
    def __create_strm_file(dest_file: str, dest_dir: str, library_dir: str):
        """
        生成strm文件
        :param library_dir:
        :param dest_dir:
        :param dest_file:
        """
        try:
            # 获取视频文件名和目录
            video_name = Path(dest_file).name
            # 获取视频目录
            dest_path = Path(dest_file).parent

            if not dest_path.exists():
                logger.info(f"创建目标文件夹 {dest_path}")
                os.makedirs(str(dest_path))

            # 构造.strm文件路径
            strm_path = os.path.join(dest_path, f"{os.path.splitext(video_name)[0]}.strm")

            logger.info(f"替换前本地路径:::{dest_file}")

            # 本地挂载路径转为emby路径
            dest_file = dest_file.replace(dest_dir, library_dir)
            logger.info(f"替换后emby容器内路径:::{dest_file}")

            # 写入.strm文件
            with open(strm_path, 'w') as f:
                f.write(dest_file)

            logger.info(f"创建strm文件 {strm_path}")
        except Exception as e:
            logger.error(f"创建strm文件失败")
            print(str(e))

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "monitor_confs": self._monitor_confs
        })

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
                   {
                       'component': 'VForm',
                       'content': [
                           {
                               'component': 'VRow',
                               'content': [
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12,
                                           'md': 6
                                       },
                                       'content': [
                                           {
                                               'component': 'VSwitch',
                                               'props': {
                                                   'model': 'enabled',
                                                   'label': '启用插件',
                                               }
                                           }
                                       ]
                                   },
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12,
                                           'md': 6
                                       },
                                       'content': [
                                           {
                                               'component': 'VSwitch',
                                               'props': {
                                                   'model': 'onlyonce',
                                                   'label': '立即运行一次',
                                               }
                                           }
                                       ]
                                   }
                               ]
                           },
                           {
                               'component': 'VRow',
                               'content': [
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12
                                       },
                                       'content': [
                                           {
                                               'component': 'VTextarea',
                                               'props': {
                                                   'model': 'monitor_confs',
                                                   'label': '监控目录',
                                                   'rows': 5,
                                                   'placeholder': '监控目录:转移目的目录:媒体服务器内路径:监控方式'
                                               }
                                           }
                                       ]
                                   }
                               ]
                           },
                           {
                               'component': 'VRow',
                               'content': [
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12,
                                       },
                                       'content': [
                                           {
                                               'component': 'VAlert',
                                               'props': {
                                                   'type': 'info',
                                                   'variant': 'tonal',
                                                   'text': '目录监控格式：'
                                                           '监控目录:目的目录:媒体服务器内路径:监控方式。'
                                               }
                                           }
                                       ]
                                   }
                               ]
                           },
                           {
                               'component': 'VRow',
                               'content': [
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12,
                                       },
                                       'content': [
                                           {
                                               'component': 'VAlert',
                                               'props': {
                                                   'type': 'info',
                                                   'variant': 'tonal',
                                                   'text': '媒体服务器内路径：'
                                                           '网盘映射本地路径挂载进媒体服务器的路径。'
                                               }
                                           }
                                       ]
                                   }
                               ]
                           },
                           {
                               'component': 'VRow',
                               'content': [
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12,
                                       },
                                       'content': [
                                           {
                                               'component': 'VAlert',
                                               'props': {
                                                   'type': 'info',
                                                   'variant': 'tonal',
                                                   'text': '监控方式：'
                                                           'fast:性能模式，内部处理系统操作类型选择最优解；'
                                                           'compatibility:兼容模式，目录同步性能降低且NAS不能休眠，但可以兼容挂载的远程共享目录如SMB'
                                               }
                                           }
                                       ]
                                   }
                               ]
                           },
                           {
                               'component': 'VRow',
                               'content': [
                                   {
                                       'component': 'VCol',
                                       'props': {
                                           'cols': 12,
                                       },
                                       'content': [
                                           {
                                               'component': 'VAlert',
                                               'props': {
                                                   'type': 'info',
                                                   'variant': 'tonal',
                                                   'text': '立即运行一次：'
                                                           '全量运行一次。'
                                               }
                                           }
                                       ]
                                   }
                               ]
                           },
                       ]
                   }
               ], {
                   "enabled": False,
                   "onlyonce": False,
                   "monitor_confs": ""
               }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    print(str(e))
        self._observer = []
