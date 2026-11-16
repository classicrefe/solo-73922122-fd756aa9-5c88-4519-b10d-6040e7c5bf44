// 删除测量列前的确认；表单其余部分由服务端渲染，版本号来自用户所见页面。
document.addEventListener("submit", (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) return;
  if (form.dataset.confirm) {
    if (!window.confirm(form.dataset.confirm)) {
      event.preventDefault();
    }
  }
});

// 批次表格在窄屏下允许横向滚动，不额外处理。
